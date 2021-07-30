package org.pipecraft.pipes.exceptions;

/**
 * Indicates an error during data fetching/processing/writing of a pipeline
 * This is the surertype for all pipe exceptions.
 * 
 * @author Eyal Schneider
 */
public abstract class PipeException extends Exception {
  private static final long serialVersionUID = 1L;
  
  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public PipeException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The nested exception
   */
  public PipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructor
   * 
   * @param cause The nested exception
   */
  public PipeException(Throwable cause) {
    super(cause);
  }
}
