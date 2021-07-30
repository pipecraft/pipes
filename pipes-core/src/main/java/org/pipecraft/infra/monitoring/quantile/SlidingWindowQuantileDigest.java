package org.pipecraft.infra.monitoring.quantile;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

/**
 * High throughput, thread safe, quantile and cdf estimation with a sliding window of time.
 * <p>
 * This structure maintains a <i>live digest</i> and a fixed capacity <i>journal</i>. The live digest deals with all
 * <code>add()</code> operations. It is then periodically appended as an <i>entry</i> to the journal and reset. If the
 * journal is at capacity, the oldest entry is removed to make way for the new one. The size of the journal determines
 * the length of the sliding window.
 * <p><code>length_of_sliding_window_in_milliseconds = capacity * journalingIntervalMillis</code></p>
 * <p>
 * To improve throughput of the add operation, the live digest is a {@link MultiQuantileDigest} which maintains multiple
 * (<code>liveDigestCount</code>) live digests simultaneously and routes <code>add()</code> requests to the different
 * digests.
 * <p><b>Example: </b></p>
 *  <ul>
 *    <li> journalingIntervalMillis: 60000
 *    <li> capacity:                 120
 *    <li> liveDigestCount:          10
 *  </ul>
 * <p>
 * The above specs would journal every minute (60000 milliseconds) for the last 120 minutes. Essentially only retaining
 * data for the last 2 hours. Calling the {@link #quantile(double) quantile} function would give data for the complete
 * 2 hour time period. If we required quantiles for the last 30 minutes, we would call
 * {@link #summarize(int) summarize(30).quantile()}.
 * <p>
 * The <code>liveDigestCount</code> of 10 determines that the {@link MultiQuantileDigest} maintains 10 live digests
 * simultaneously. Upon querying ({@link #quantile(double) quantile}, {@link #cdf(double) cdf}, ...), the
 * <code>MultiQuantileDigest</code> along with the journal are {@link #summarize() summarize}d into one single
 * {@link QuantileDigest}, which then responds to all quantile and cdf queries.
 * <p>
 * At journaling time, the 10 live digests are merged and appended as 1 entry to the journal.
 *
 * @author Mojtaba Kohram
 */
public class SlidingWindowQuantileDigest implements ConcurrentQuantileEstimator {
  private static final double COMPRESSION_INFLATION_DEFAULT = 1.5;
  private static final double COMPRESSION_DEFAULT = 100;
  private final int journalingIntervalMillis;
  private final double compression;
  private final MultiQuantileDigest liveDigest;
  private final QuantileDigest[] journal;
  private volatile int journalPosition = 0;
  private final Object lock = new Object();

  /**
   * Instantiate with default compression and default compressionInflation values
   *
   * @param liveDigestCount           the number of live digests to maintain
   * @param journalingIntervalMillis  interval between two journaling operations in milliseconds, equivalent to the
   *                                  resolution of the sliding window, set this to -1 to disable automated journaling
   * @param capacity                  capacity of the journal
   * @param executorService           the executor to schedule the journaling task to
   */
  public SlidingWindowQuantileDigest(
          int liveDigestCount, int journalingIntervalMillis, int capacity, ScheduledExecutorService executorService) {
    this(liveDigestCount, COMPRESSION_DEFAULT, COMPRESSION_INFLATION_DEFAULT, journalingIntervalMillis, capacity,
            executorService);
  }

  /**
   * Instantiate with default compressionInflation value
   *
   * @param liveDigestCount           the number of live digests to maintain
   * @param compression               the compression factor of the final digest
   * @param journalingIntervalMillis  interval between two journaling operations in milliseconds, equivalent to the
   *                                  resolution of the sliding window, set this to -1 to disable automated journaling
   * @param capacity                  capacity of the journal
   * @param executorService           the executor to schedule the journaling task to
   */
  public SlidingWindowQuantileDigest(int liveDigestCount, double compression, int journalingIntervalMillis, int capacity,
                                     ScheduledExecutorService executorService) {
    this(liveDigestCount, compression, COMPRESSION_INFLATION_DEFAULT, journalingIntervalMillis, capacity,
            executorService);
  }

  /**
   * Fully specified constructor
   *
   * @param liveDigestCount                 the number of live digests to maintain
   * @param compression                     the compression factor of the final digest
   * @param compressionInflationMultiplier  the compression inflation multiplier,
   *                                        see: {@link MultiQuantileDigest#getCompressionInflation()}
   * @param journalingIntervalMillis        interval between two journaling operations in milliseconds, equivalent to
   *                                        the resolution of the sliding window, set this to -1 to disable automated
   *                                        journaling
   * @param capacity                        capacity of the journal
   * @param executorService                 the executor to schedule the journaling task to
   */
  public SlidingWindowQuantileDigest(
          int liveDigestCount, double compression, double compressionInflationMultiplier, int journalingIntervalMillis,
          int capacity, ScheduledExecutorService executorService) {
    this.journalingIntervalMillis = journalingIntervalMillis;
    this.compression = compression;

    liveDigest = new MultiQuantileDigest(liveDigestCount, compression * compressionInflationMultiplier);
    journal = new QuantileDigest[capacity];

    for (int i = 0; i < journal.length; i ++) {
      journal[i] = new QuantileDigest(compression * compressionInflationMultiplier);
    }

    if (journalingIntervalMillis != -1) {
      executorService.scheduleAtFixedRate(this::publishToJournal, journalingIntervalMillis, journalingIntervalMillis,
              TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Get the compression factor.
   *gi
   * @return  the compression factor
   */
  public double getCompression() {
    return compression;
  }

  /**
   * Gets journaling interval. Returns -1 if automated journaling is disabled.
   *
   * @return  the journaling interval in milliseconds
   */
  public int getJournalingIntervalMillis() {
    return journalingIntervalMillis;
  }

  /**
   * Gets the journal capacity.
   *
   * @return  the capacity of the journal
   */
  public int getCapacity() {
    return journal.length;
  }

  /**
   * Get an aggregate view of the current state of this object as a <code>QuantileDigest</code>. The returned
   * <code>QuantileDigest</code> is independent of this object and the caller is free to modify the returned
   * digest.
   *
   * @return  the digest representing all the data currently consumed by this object
   */
  public QuantileDigest summarize() {
    return summarize(journal.length);
  }

  /**
   * Get an aggregate view of the current state of the <code>lookback</code> most recent journal entries as a
   * <code>QuantileDigest</code>. In other words, journal entries are added in reverse chronological order.
   * The returned <code>QuantileDigest</code> is independent of this object and the caller is free to modify
   * the returned digest.
   *
   * @param lookback the number of journal entries to add to the summary, must be less than the journal capacity
   * @return  the digest representing <code>lookback</code> journal entries
   */
  public QuantileDigest summarize(int lookback) {
    Validate.isTrue(lookback <= journal.length, "lookback must be less than or equal to capacity");

    QuantileDigest summary = new QuantileDigest(compression);

    int startPos = (journalPosition - lookback + journal.length) % journal.length;
    for (int i = 0; i < lookback; i++) {
      summary.add(journal[(startPos + i) % journal.length]);
    }

    summary.add(liveDigest.summarize());

    return summary;
  }

  /**
   * Squash live digest(s) and add to journal as latest entry. If journal is at capacity removes oldest entry.
   */
  public void publishToJournal()  {
    synchronized (lock) {
      int currentPos = journalPosition % journal.length;
      journalPosition++;

      journal[currentPos].reset();
      journal[currentPos].add(liveDigest.reset());
    }
  }

  /**
   * {@inheritDoc}
   */
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
  public long size() {
    return summarize().size();
  }

  /**
   * {@inheritDoc}
   */
  public double cdf(double x) {
    return summarize().cdf(x);
  }

  /**
   * {@inheritDoc}
   */
  public List<Double> cdf(List<Double> coords) {
    QuantileDigest digest = summarize();

    List<Double> cdfs = new ArrayList<>(coords.size());
    for (Double x: coords) {
      cdfs.add(digest.cdf(x));
    }

    return cdfs;
  }

  /**
   * {@inheritDoc}
   */
  public boolean tryAdd(double x, int w) {
    return liveDigest.tryAdd(x, w);
  }

  /**
   * Add a weighted sample. This is a blocking call, for non-blocking adds see {@link #tryAdd(double, int)}.
   *
   * @param x  data to add
   * @param w  weight
   */
  public void add(double x, int w) {
    liveDigest.add(x, w);
  }
}
