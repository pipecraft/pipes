package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.TableExportConfig;

/**
 * An exception indicating that the export request is illegal
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class InvalidExportBQException extends NonTransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param config The illegal export configuration
   */
  public InvalidExportBQException(String msg, TableExportConfig config) {
    super(msg + "\n" + config);
  }
}
