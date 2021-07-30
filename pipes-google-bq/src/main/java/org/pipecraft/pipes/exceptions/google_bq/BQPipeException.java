package org.pipecraft.pipes.exceptions.google_bq;

import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe exception due to BigQuery errors
 * 
 * @author Eyal Schneider
 */
public class BQPipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  
  public BQPipeException(String msg) {
    super(msg);
  }
  
  public BQPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public BQPipeException(Throwable cause) {
    super(cause);
  }
}
