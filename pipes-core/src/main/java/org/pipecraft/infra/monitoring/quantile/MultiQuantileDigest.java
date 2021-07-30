package org.pipecraft.infra.monitoring.quantile;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;

/**
 * Thread Safe high-throughput quantile and cdf estimation.
 * <p>
 * Maintains multiple (<code>digestCount</code>) {@link QuantileDigest}s and responds to quantile queries by
 * merging the digests into a single digest and querying the merged (i.e. {@link #summarize() summarized}) digest. This
 * increases the throughput of the <code>QuantileDigest</code> by routing add operations to the different
 * digests. A rule thumb for setting <code>digestCount</code>:
 *
 * <p><code>no_of_write_threads &lt; digestCount &lt; 2 * no_of_write_threads</code></p>
 *
 * Most applications would be fine with MUCH smaller numbers than that. Set <code>digestCount = 1</code> for single
 * threaded apps. With <code>digestCount = 1</code>, this should be equivalent to a
 * <code>QuantileDigest</code> with some overhead.
 * <p>
 * When merging multiple digests, it is recommended that the individual digests have a higher compression factor than
 * the final merged digest. This leads to a more accurate merged digest. The compression factor of each of
 * the internal digests is:
 *    <p><code>compressionInflation * compression</code></p>
 * The merged digest that responds to queries has a compression factor equal to <code>compression</code>. Empirical
 * results show that a compression value of 100 performs well for most use cases. The average serialized size of each
 * {@link QuantileDigest} with compression 100 is less than 1 KB.
 *
 * @author Mojtaba Kohram
 */
public class MultiQuantileDigest implements ConcurrentQuantileEstimator {
  private static final double COMPRESSION_INFLATION_DEFAULT = 1.5;
  private static final double COMPRESSION_DEFAULT = 100;

  private final List<QuantileDigest> digests;
  private final int digestCount;
  private final double compression;
  private final double compressionInflation;
  private final AtomicLong roundRobinIndex = new AtomicLong();

  /**
   * Constructor with default values
   *
   * @param digestCount  number of digests
   */
  public MultiQuantileDigest(int digestCount) {
    this(digestCount, COMPRESSION_DEFAULT, COMPRESSION_INFLATION_DEFAULT);
  }

  /**
   * Constructor with default values
   *
   * @param digestCount  number of digests to keep
   * @param compression  compression factor of each digest
   */
  public MultiQuantileDigest(int digestCount, double compression) {
    this(digestCount, compression, COMPRESSION_INFLATION_DEFAULT);
  }

  /**
   * Fully specified constructor
   *
   * @param digestCount  number of digests to keep
   * @param compression  compression factor of final digest
   * @param compressionInflationMultiplier  compression factor multiplier
   */
  public MultiQuantileDigest(int digestCount, double compression, double compressionInflationMultiplier) {
    Validate.isTrue(digestCount > 0, "digestCount must be greater than 0");
    Validate.isTrue(compression > 0, "compression must be greater than 0");
    Validate.isTrue(compressionInflationMultiplier > 1, "inflationMultiplier must be greater than 1");

    this.digestCount = digestCount;
    this.compression = compression;
    this.compressionInflation = compressionInflationMultiplier;

    this.digests = new ArrayList<>(digestCount);
    for (int i = 0; i < digestCount; i++) {
      digests.add(new QuantileDigest(compression * compressionInflationMultiplier));
    }
  }

  /**
   * The compression factor of each internal digest is equal to
   * <code>{@link #compressionInflation} * {@link #compression}</code>
   * @return  the compression inflation factor
   */
  public double getCompressionInflation() {
    return compressionInflation;
  }

  /**
   * The compression factor
   *
   * @return  the compression factor
   */
  public double getCompression() {
    return compression;
  }

  /**
   * Reset the digest. Returns a {@link QuantileDigest} representing the final state of this object prior to
   * reset.
   *
   * @return  a digest representing the final state of this object prior to reset
   */
  public QuantileDigest reset() {
    List<QuantileDigest> finalState = new ArrayList<>(digestCount);
    for (QuantileDigest digest: digests) {
      finalState.add(digest.reset());
    }

    QuantileDigest digest = new QuantileDigest(compression);
    digest.add(finalState);

    return digest;
  }

  /**
   * Merge internal digests into a single {@link QuantileDigest}. The returned
   * {@link QuantileDigest} is independent of this object and the caller is free to modify it.
   *
   * @return  the merged digest
   */
  public QuantileDigest summarize() {
    QuantileDigest digest = new QuantileDigest(compression);
    digest.add(digests);

    return digest;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double quantile(double q) {
    return summarize().quantile(q);
  }

  /**
   * {@inheritDoc}
   */
  public List<Double> quantile(List<Double> qs) {
    QuantileDigest digest = summarize();

    List<Double> quantiles = new ArrayList<>(qs.size());
    for (Double q: qs) {
      quantiles.add(digest.quantile(q));
    }

    return quantiles;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long size() {
    return summarize().size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double cdf(double x) {
    return summarize().cdf(x);
  }

  /**
   * {@inheritDoc}
   */
  public List<Double> cdf(List<Double> coords) {
    QuantileDigest digest = summarize();

    List<Double> quantiles = new ArrayList<>(coords.size());
    for (Double x: coords) {
      quantiles.add(digest.cdf(x));
    }

    return quantiles;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean tryAdd(double x, int w) {
    int digest_idx = (int) (roundRobinIndex.incrementAndGet() % digestCount);

    return digests.get(digest_idx).tryAdd(x, w);
  }

  /**
   * Add a weighted sample. This is a blocking call, for non-blocking adds see {@link #tryAdd(double, int)}.
   *
   * @param x  data to add
   * @param w  weight
   */
  @Override
  public void add(double x, int w) {
    int digest_idx = (int) (roundRobinIndex.incrementAndGet() % digestCount);
    digests.get(digest_idx).add(x, w);
  }
}
