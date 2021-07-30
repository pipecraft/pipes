package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * A transient exception indicating that a BQ request timed out
 * (Suitable also for low level socket read timeout)
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class TimeoutBQException extends TransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param query The query where the timeout was found
   */
  public TimeoutBQException(String msg, BQQuery<?, ?> query) {
    super(msg, query);
  }

  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public TimeoutBQException(String msg) {
    super(msg);
  }

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param e The cause
   * @param query The query where the timeout was found
   */
  public TimeoutBQException(String msg, Throwable e, BQQuery<?, ?> query) {
    super(msg, e, query);
  }

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param e The cause
   */
  public TimeoutBQException(String msg, Throwable e) {
    super(msg, e);
  }
}
