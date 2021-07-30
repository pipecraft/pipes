package org.pipecraft.infra.monitoring.quantile;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A synchronized wrapper for {@link com.tdunning.math.stats.MergingDigest com.tdunning.math.stats.MergingDigest} which
 * is an implementation of t-digests. The t-digest data structure is used for accurate quantile and cdf estimation.
 * <p>
 * All read operations on the wrapped {@link com.tdunning.math.stats.MergingDigest}
 * (<code>size()</code>, <code>quantile()</code>, <code>byteSize()</code>, ...) change its internal state arnd are also
 * synchronized.
 * <p>
 * The digest <i>compression factor</i> is the size/accuracy tradeoff parameter of t-digests. The memory requirements
 * are <code>Θ(compression)</code>. Empirical results show a value of 100 is pretty good. The average serialized size
 * of a digest with compression 100 is less than 1 KB.
 * <p>
 * @see <a href="https://github.com/tdunning/t-digest">https://github.com/tdunning/t-digest</a>
 * @see <a href="https://arxiv.org/abs/1902.04023">https://arxiv.org/abs/1902.04023</a>
 *
 * @author Mojtaba Kohram
 */
public class QuantileDigest implements ConcurrentQuantileEstimator {
  private MergingDigest digest;
  private final double compression;
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Constructor
   *
   * @param compression  the compression factor
   */
  public QuantileDigest(double compression) {
    this.digest = new MergingDigest(compression);
    this.compression = compression;
  }

  /**
   * Wrap a {@link com.tdunning.math.stats.MergingDigest} in order to make it thread safe. The resulting object is
   * independent of the original <code>MergingDigest</code> and could be thought of as a copy of the original digest.
   * Could display erratic behaviour if the <code>MergingDigest</code> is modified while wrapping is in progress.
   *
   * @param digest  the digest to wrap
   * @return  the synchronized merging digest
   */
  public static QuantileDigest wrap(MergingDigest digest) {
    QuantileDigest wrapped = new QuantileDigest(digest.compression());

    wrapped.add(Collections.singletonList(new QuantileDigest(digest)));

    return wrapped;
  }

  /**
   * Initialize from a serialized buffer.
   *
   * @param buff  the buffer to construct from
   * @return  the synchronized merging digest
   */
  public static QuantileDigest fromBytes(ByteBuffer buff) {
    return new QuantileDigest(MergingDigest.fromBytes(buff));
  }

  public double getCompression() {
    return compression;
  }

  /**
   * Merge a list of <code>QuantileDigest</code> into this digest.
   *
   * @param others  the digests to merge into this digest
   */
  public void add(List<QuantileDigest> others) {
    if (others.size() == 0) {
      return;
    }

    List<MergingDigest> copies = new ArrayList<>();
    for (QuantileDigest other: others) {
      if (other.size() > 0) {
        copies.add(other.getDigestCopy());
      }
    }

    lock.lock();
    try {
      this.digest.add(copies);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Merge another <code>QuantileDigest</code> into this digest.
   *
   * @param other  the other digest to merge
   */
  public void add(QuantileDigest other) {
    add(Collections.singletonList(other));
  }

  /**
   * Serialize this object to a read-only {@link ByteBuffer}.
   *
   * @return  this object serialized to a read-only {@link ByteBuffer}
   */
  public ByteBuffer asReadOnlyBuffer() {
    lock.lock();
    try {
      ByteBuffer buffer = ByteBuffer.allocate(digest.byteSize());
      digest.asBytes(buffer);

      ByteBuffer readOnly = buffer.asReadOnlyBuffer();
      readOnly.rewind();

      return readOnly;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Resets the digest to size zero and returns the final state prior to resetting.
   *
   * @return  final state of this object prior to resetting
   */
  public QuantileDigest reset() {
    lock.lock();
    try {
      QuantileDigest current = new QuantileDigest(getDigestCopy());
      this.digest = new MergingDigest(compression);

      return current;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Compression factor of this digest.
   *
   * @return  the compression factor
   */
  public double compression() {
    return compression;
  }

  /**
   * Returns the byte size of the wrapped <code>MergingDigest</code>.
   *
   * @return  the byte size of the wrapped <code>MergingDigest</code>
   */
  public int byteSize() {
    lock.lock();
    try {
      return digest.byteSize();
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long size() {
    lock.lock();
    try {
      return digest.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double cdf(double x) {
    lock.lock();
    try {
      return digest.cdf(x);
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Double> cdf(List<Double> coords) {
    lock.lock();
    try {
      List<Double> cdfs = new ArrayList<>(coords.size());
      for (Double x: coords) {
        cdfs.add(digest.cdf(x));
      }

      return cdfs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double quantile(double q) {
    lock.lock();
    try {
      return digest.quantile(q);
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Double> quantile(List<Double> qs) {
    lock.lock();
    try {
      List<Double> quantiles = new ArrayList<>(qs.size());
      for (Double x: qs) {
        quantiles.add(digest.quantile(x));
      }

      return quantiles;
    } finally {
      lock.unlock();
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean tryAdd(double x, int w) {
    boolean locked = lock.tryLock();
    if (locked) {
      try {
        digest.add(x, w);
      } finally {
        lock.unlock();
      }
    }

    return locked;
  }

  /**
   * Adds a weighted sample to the digest. Locks the object until data is added. For non-blocking adds, see
   * {@link #tryAdd(double, int)}.
   *
   * @param x  data to add
   * @param w  weights
   */
  @Override
  public void add(double x, int w) {
    lock.lock();
    try {
      digest.add(x, w);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Unsafe constructor. Should only be used if we are sure that the incoming digest does not have any pointers from
   * elsewhere that could modify it.
   *
   * @param digest  the digest to synchronize
   */
  private QuantileDigest(MergingDigest digest) {
    this.digest = digest;
    this.compression = digest.compression();
  }

  private MergingDigest getDigestCopy() {
    lock.lock();
    try {
      ByteBuffer buffer = asReadOnlyBuffer();
      return MergingDigest.fromBytes(buffer);
    } finally {
      lock.unlock();
    }
  }
}
