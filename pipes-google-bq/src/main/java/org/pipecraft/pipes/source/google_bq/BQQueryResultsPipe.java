package org.pipecraft.pipes.source.google_bq;

import org.pipecraft.infra.bq.BQQuery;
import org.pipecraft.infra.bq.BQResultsIterator;
import org.pipecraft.infra.bq.BigQueryConnector;
import org.pipecraft.infra.bq.QueryExecutionConfig;
import org.pipecraft.infra.bq.exceptions.BQException;
import org.pipecraft.infra.bq.exceptions.QueryResultBrokenException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.google_bq.BQPipeException;
import java.io.IOException;

/**
 * A source pipe that runs a query on {@link BigQueryConnector} and iterates over the results
 * 
 * @param <T> The type of each result record from BQ
 * 
 * @author Eyal Schneider
 */
public class BQQueryResultsPipe<T> implements Pipe<T> {
  private final BigQueryConnector bq;
  private final BQQuery<T, ?> query;
  private final QueryExecutionConfig queryConfig;
  private BQResultsIterator<T, ?> it;
  private T next;
  private volatile long totalCount;
  private volatile long iterCount;
  
  /**
   * Constructor
   * @param bq The BQ connector
   * @param query The query to execute
   * @param queryConfig Query execution configuration, or null for the default one
   */
  public BQQueryResultsPipe(BigQueryConnector bq, BQQuery<T, ?> query, QueryExecutionConfig queryConfig) {
    this.bq = bq;
    this.query = query;
    this.queryConfig = queryConfig;
  }
  
  @Override
  public void close() throws IOException {
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T res = next;
    prepareNext();
    iterCount++;
    return res;
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      if (queryConfig == null) {
        it = bq.execute(query);
      } else {
        it = bq.execute(query, queryConfig);
      }
      this.totalCount = it.totalRecordCount();
      prepareNext();
    } catch (BQException e) {
      throw new BQPipeException(e);
    }
  }

  public void prepareNext() throws BQPipeException {
    try {
      if (it.hasNext()) {
        next = it.next();
      } else {
        next = null;
      }
    } catch (QueryResultBrokenException e) {
      throw new BQPipeException(e);
    }
  }

  @Override
  public float getProgress() {
    if (totalCount == 0) {
      return 1.0f;
    }
    return iterCount / (float)totalCount;
  }
}
