package org.pipecraft.infra.bq;

import org.pipecraft.infra.bq.exceptions.BQException;
import org.pipecraft.infra.bq.exceptions.QueryResultBrokenException;
import org.pipecraft.infra.bq.exceptions.TransientBQException;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * A decorator on BQ result set iterator. Provides: 
 * 1) Total row count 
 * 2) A method for aggregating all rows using the originating query's aggregation logic.
 * 3) Conversion of runtime errors during iteration to the standard QueryResultBrokenException.
 * 
 * @author Eyal Schneider
 *
 * @param <R> The row object data type
 * @param <F> The aggregated result data type
 */
public class BQResultsIterator<R, F> implements Iterator<R> {
  private final Iterator<R> iterator;
  private final long recordCount;
  private final BQQuery<R, F> query;

  /**
   * Package protected constructor
   * 
   * @param query The query originating this result
   * @param iterator The results iterator to wrap
   * @param recordCount The total number of records in the result set
   */
  BQResultsIterator(BQQuery<R,F> query, Iterator<R> iterator, long recordCount) {
    this.query = query;
    this.iterator = iterator;
    this.recordCount = recordCount;
  }

  @Override
  public boolean hasNext() {
    try {
      return iterator.hasNext();
    } catch (Exception e) { // Not ideal, but Google's API is badly documented so we don't know what to expect in case of pagination errors
      throw new QueryResultBrokenException(e);
    }
  }

  @Override
  public R next() {
    try {
      return iterator.next();
    } catch (Exception e) {
      throw new QueryResultBrokenException(e); // Not ideal, but Google's API is badly documented so we don't know what to expect in case of pagination errors
    }
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void forEachRemaining(Consumer<? super R> action) {
    try {
      iterator.forEachRemaining(action);
    } catch (Exception e) {
      throw new QueryResultBrokenException(e); // Not ideal, but Google's API is badly documented so we don't know what to expect in case of pagination errors
    }
  }

  /**
   * @return The total number of records in the result set
   */
  public long totalRecordCount() {
    return recordCount;
  }
  
  /**
   * Call this method only if the originating query supports aggregation.
   * @return The final result object, obtained by aggregating all iterator's rows
   * @throws BQException In case of a BigQuery error during iteration, or if
   * in case of a logical error during aggregation.
   */
  public F aggregate() throws BQException {
    try {
      return query.aggregate(this);
    } catch (QueryResultBrokenException e) {
      throw new TransientBQException("Failed iterating over query results", e);
    }
  }
}
