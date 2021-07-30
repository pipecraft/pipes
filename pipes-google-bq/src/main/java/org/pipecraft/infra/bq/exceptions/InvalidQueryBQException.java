package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * An exception indicating that a query is illegal
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class InvalidQueryBQException extends NonTransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param query The failed query
   */
  public InvalidQueryBQException(String msg, BQQuery<?,?> query) {
    super(msg, query);
  }
}
