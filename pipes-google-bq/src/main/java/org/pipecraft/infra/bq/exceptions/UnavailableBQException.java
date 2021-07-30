package org.pipecraft.infra.bq.exceptions;

/**
 * An exception indicating that BQ service is temporarily unavailable
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class UnavailableBQException extends TransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public UnavailableBQException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The original exception
   */
  public UnavailableBQException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
