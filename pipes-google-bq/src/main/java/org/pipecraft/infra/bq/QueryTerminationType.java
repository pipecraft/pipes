package org.pipecraft.infra.bq;

/**
 * The different termination types for a BQ query
 * 
 * @author Eyal Schneider
 */
enum QueryTerminationType {
  SUCCESS, 
  FAILED_INVALID_QUERY, // The query has an illegal syntax or refers to non existing resources 
  FAILED_TIMEOUT, // Timeout while waiting for response
  FAILED_CLIENT_RESOURCES_LIMIT, // Client failed the request due to client side resources limit
  FAILED_SERVER_RESOURCES_LIMIT, // Server failed the request due to too many resources needed for the query execution
  FAILED_CLIENT_TOO_MANY_ROWS, // Too many result rows fetched. This is a soft limit (client side config)
  FAILED_SERVER_TOO_MANY_ROWS, // Too many result rows fetched. This is a hard limit (BQ server side).
  FAILED_SERVER_ERROR, // Internal error in BQ Server
  FAILED_OTHER, // Other error type, not specified above
  INTERNAL_ERROR // Indicates an unexpected error at our side (bug)
}
