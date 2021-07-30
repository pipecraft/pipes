package org.pipecraft.pipes.exceptions;

/**
 * A pipe exception due to JDBC related errors
 * 
 * @author Eyal Schneider
 */
public class JdbcPipeException extends PipeException {
  private static final long serialVersionUID = 1L;
  
  public JdbcPipeException(String msg) {
    super(msg);
  }
  
  public JdbcPipeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public JdbcPipeException(Throwable cause) {
    super(cause);
  }
}
