package org.pipecraft.pipes.exceptions;

/**
 * A pipe exception resulting from a runtime error.
 * Usually used for communicating the error to another thread, so that the corresponding runtime exception can be thrown in that thread.
 * 
 * @author Eyal Schneider
 */
public class InternalPipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  private final RuntimeException rtException;
  
  public InternalPipeException(String msg) {
    super(msg);
    this.rtException = null;
  }
  
  public InternalPipeException(RuntimeException cause) {
    super(cause);
    this.rtException = cause;
  }
  
  /**
   * @return The runtime exception being wrapped. Intended to be thrown in the right context.
   */
  public RuntimeException getRuntimeException() {
    return rtException;
  }
}
