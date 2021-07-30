package org.pipecraft.infra.storage;

/**
 * A storage exception meaning that a remote json file has illegal structure and can't be deserialized properly.
 * 
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class IllegalJsonException extends Exception {

  public IllegalJsonException(String message) {
    super(message);
  }

  public IllegalJsonException(String message, Throwable t) {
    super(message, t);
  }
}
