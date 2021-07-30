package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.TableLoadConfig;

/**
 * An exception indicating that the table load request is illegal
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class InvalidTableLoadBQException extends NonTransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param config The illegal load configuration
   */
  public InvalidTableLoadBQException(String msg, TableLoadConfig config) {
    super(msg + "\n" + config);
  }
}
