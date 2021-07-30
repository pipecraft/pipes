package org.pipecraft.pipes.exceptions;

/**
 * An IO pipe exception indicating an error reported by an HTTP server
 * Includes the HTTP status code.
 * 
 * @author Eyal Schneider
 */
public class HttpPipeException extends IOPipeException {
  private static final long serialVersionUID = 1L;
  private final int statusCode;
  
  public HttpPipeException(int statusCode) {
    super(buildMessage(statusCode));
    this.statusCode = statusCode;
  }
  
  public HttpPipeException(int statusCode, Throwable cause) {
    super(buildMessage(statusCode), cause);
    this.statusCode = statusCode;
  }
  
  /**
   * @return the HTTP status code indicating the error type
   */
  public int getStatusCode() {
    return statusCode;
  }
  
  private static String buildMessage(int statusCode) {
    return "HTTP error with status code " + statusCode;
  }
}
