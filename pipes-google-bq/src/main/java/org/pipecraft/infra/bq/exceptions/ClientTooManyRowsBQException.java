package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * An exception indicating that the total number of rows in the BQ response exceeds the
 * soft limit (client side)
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class ClientTooManyRowsBQException extends NonTransientBQException {

  /**
   * Constructor
   *
   * @param msg The error message
   * @param query The failed query
   */
  public ClientTooManyRowsBQException(String msg, BQQuery<?,?> query) {
    super(msg, query);
  }
}
