package org.pipecraft.infra.monitoring.quantile;

import java.util.List;

/**
 * Thread-safe quantile and cdf estimation
 *
 * @author Mojtaba Kohram
 */
public interface ConcurrentQuantileEstimator {
  /**
   * Returns the number of samples added to the current Estimator.
   *
   * @return  the number of samples currently added
   */
  long size();

  /**
   * Get an estimate for the cdf of the distribution at <code>x</code>. This function could be expensive depending on
   * implementation. Consider using {@link #cdf(List)} when querying more than one cdf value.
   *
   * @param x  the value to get cdf at
   * @return  the estimated cumulative distribution function value at <code>x</code>, always between 0 and 1
   */
  double cdf(double x);

  /**
   * Get an estimate for the cdf of the distribution at every coordinate of input list.
   *
   * @param coordinates  the list of coordinates to compute the cdf at
   * @return  the estimated cumulative distribution function value for every element in <code>coordinates</code>, in
   * order, results are always between 0 and 1
   */
  List<Double> cdf(List<Double> coordinates);

  /**
   * Get an estimate of the quantile at <code>q</code>. This function could be expensive depending on implementation.
   * Consider using {@link #quantile(List)} when querying more than one quantile value.
   *
   * @param q  quantile to query, must be between 0 and 1
   * @return  the estimated quantile value at <code>q</code>
   */
  double quantile(double q);

  /**
   * Get an estimated quantile for every value in the input list.
   *
   * @param quantileList  list of quantiles to query, must all be between 0 and 1
   * @return  the estimated quantile value for every element in <code>quantileList</code>, in order
   */
  List<Double> quantile(List<Double> quantileList);

  /**
   * Attempts to add a weighted sample to this estimator. Returns false if a lock is held by another thread.
   *
   * @param x  data to add
   * @param w  weight
   * @return  false if this object's lock is held by another thread, true otherwise
   */
  boolean tryAdd(double x, int w);

  /**
   * Attempts to add an unweighted sample to this estimator. Returns false if a lock is held by another thread.
   *
   * @param x  data to add
   * @return  false if this objects lock is held by another thread, true otherwise
   */
  default boolean tryAdd(double x) {
    return tryAdd(x, 1);
  }

  /**
   * Add a weighted sample. This function could block to ensure the data is added to the estimator, for non-blocking
   * adds see {@link #tryAdd(double, int)}.
   *
   * @param x  data to add
   * @param w  weight
   */
  void add(double x, int w);

  /**
   * Add an unweighted sample to this estimator. This function could block to ensure the data is added to the estimator,
   * for non-blocking adds see {@link #tryAdd(double)}.
   *
   * @param x  data to add
   */
  default void add(double x) {
    add(x, 1);
  }
}
