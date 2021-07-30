package org.pipecraft.pipes.exceptions;

/**
 * Indicates an error found during the pipeline while interacting with
 * producer/consumer queues.
 * 
 * @author Eyal Schneider
 *
 */
public class QueuePipeException extends PipeException {
 private static final long serialVersionUID = 1L;
  
  public QueuePipeException(String msg) {
    super(msg);
  }
  
  public QueuePipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public QueuePipeException(Throwable cause) {
    super(cause);
  }

}
