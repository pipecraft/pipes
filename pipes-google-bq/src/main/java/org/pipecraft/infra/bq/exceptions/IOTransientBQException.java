package org.pipecraft.infra.bq.exceptions;

/**
 * Indicates a transient IO error when writing/reading data to/from BQ.
 * May be caused by client side or server side.
 * 
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class IOTransientBQException extends TransientBQException{

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The original exception
   */
  public IOTransientBQException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
