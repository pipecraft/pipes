package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * A superclass for BQException indicating a transient situation,
 * meaning that retries may be helpful.
 * 
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class TransientBQException extends BQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public TransientBQException(String msg) {
    super(msg);
  }

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The original exception
   */
  public TransientBQException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   * 
   * @param cause The cause of this exception
   * @param query The failed query.
   */
  public TransientBQException(Throwable cause, BQQuery<?, ?> query) {
    super(cause, query);
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   * 
   * @param msg The error message
   * @param query The failed query
   */
  public TransientBQException(String msg, BQQuery<?, ?> query) {
    super(msg, query);
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   *
   * @param msg The error message
   * @param cause The cause of this exception
   * @param query The failed query
   */
  public TransientBQException(String msg, Throwable cause, BQQuery<?, ?> query) {
    super(msg, cause, query);
  }

}
