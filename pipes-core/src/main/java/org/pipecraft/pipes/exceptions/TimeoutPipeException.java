package org.pipecraft.pipes.exceptions;

/**
 * A pipe exception resulting from a timeout
 * 
 * @author Eyal Schneider
 */
public class TimeoutPipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  
  public TimeoutPipeException(String msg) {
    super(msg);
  }
  
  public TimeoutPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public TimeoutPipeException(Throwable cause) {
    super(cause);
  }
}
