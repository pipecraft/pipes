package org.pipecraft.pipes.exceptions;

/**
 * Indicates that the ordering of items in a pipe was not as expected
 * 
 * @author Eyal Schneider
 */
public class OutOfOrderPipeException extends PipeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public OutOfOrderPipeException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The nested exception
   */
  public OutOfOrderPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructor
   * 
   * @param cause The nested exception
   */
  public OutOfOrderPipeException(Throwable cause) {
    super(cause);
  }
}
