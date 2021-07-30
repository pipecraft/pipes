package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * An exception indicating that the total number of rows in the BQ response exceeds the
 * hard limit (server side)
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class ServerTooManyRowsBQException extends NonTransientBQException {

  /**
   * Constructor
   *
   * @param msg The error message
   * @param query The failed query
   */
  public ServerTooManyRowsBQException(String msg, BQQuery<?,?> query) {
    super(msg, query);
  }

  /**
   * Constructor
   *
   * @param cause The original exception
   * @param query The failed query
   */
  public ServerTooManyRowsBQException(Throwable cause, BQQuery<?,?> query) {
    super(cause, query);
  }

}
