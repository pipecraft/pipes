package org.pipecraft.pipes.exceptions;

/**
 * A pipe exception resulting from an IO error
 * 
 * @author Eyal Schneider
 */
public class IOPipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  
  public IOPipeException(String msg) {
    super(msg);
  }
  
  public IOPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public IOPipeException(Throwable cause) {
    super(cause);
  }
}
