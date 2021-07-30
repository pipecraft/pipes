package org.pipecraft.pipes.exceptions;

/**
 * A pipe exception due to resource usage beyond some threshold 
 * 
 * @author Eyal Schneider
 */
public class ExcessiveResourcesUsagePipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  
  public ExcessiveResourcesUsagePipeException(String msg) {
    super(msg);
  }
  
  public ExcessiveResourcesUsagePipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public ExcessiveResourcesUsagePipeException(Throwable cause) {
    super(cause);
  }
}
