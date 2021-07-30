package org.pipecraft.infra.monitoring.graphite;

/**
 * Graphite exception
 * @author Michal Rockban
 */
public class GraphiteException extends Exception {
  private static final long serialVersionUID = 1L;

  public GraphiteException(String message) {
    super(message);
  }

  public GraphiteException(String message, Throwable cause) {
    super(message, cause);
  }
}