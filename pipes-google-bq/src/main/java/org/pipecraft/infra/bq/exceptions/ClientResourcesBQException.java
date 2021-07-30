package org.pipecraft.infra.bq.exceptions;

/**
 * A transient exception indicating that the BQ client has too many pending requests and
 * can't serve any additional requests
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class ClientResourcesBQException extends TransientBQException {

  /**
   * Constructor
   */
  public ClientResourcesBQException() {
    super("BQ client queue overloaded");
  }
}
