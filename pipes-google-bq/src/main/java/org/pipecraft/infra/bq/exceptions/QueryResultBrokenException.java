package org.pipecraft.infra.bq.exceptions;

import java.util.Iterator;

/**
 * An unchecked exception indicating that the BQ result set iterator can't continue due to a failure
 * to retrieve more data from BQ server.
 * We must use runtime exception in order to conform with {@link Iterator}'s API, which
 * doesn't allow checked exceptions.
 * 
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class QueryResultBrokenException extends RuntimeException {
  public QueryResultBrokenException(Throwable e) {
    super(e);
  }
  
  public QueryResultBrokenException(String msg) {
    super(msg);
  }
}
