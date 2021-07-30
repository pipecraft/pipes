package org.pipecraft.pipes.exceptions;

/**
 * Indicates that the contents of a pipe were found to be invalid
 * 
 * @author Eyal Schneider
 */
public class ValidationPipeException extends PipeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor
   * 
   * @param msg The error message
   */
  public ValidationPipeException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The nested exception
   */
  public ValidationPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructor
   * 
   * @param cause The nested exception
   */
  public ValidationPipeException(Throwable cause) {
    super(cause);
  }
}
