package org.pipecraft.infra.bq.exceptions;

import org.pipecraft.infra.bq.BQQuery;

/**
 * An exception indicating that the server rejected a query
 * due to usage of too many resources
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public class ServerResourcesBQException extends NonTransientBQException {

  /**
   * Constructor
   * 
   * @param msg The error message
   * @param cause The original exception
   * @param The failed query
   */
  public ServerResourcesBQException(String msg, Throwable cause, BQQuery<?,?> query) {
    super(msg, cause, query);
  }
  
  /**
   * Constructor
   * 
   * @param cause The original exception
   * @param The failed query
   */
  public ServerResourcesBQException(Throwable cause, BQQuery<?,?> query) {
    super(cause, query);
  }

}
