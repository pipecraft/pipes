package org.pipecraft.infra.bq;

/**
 * The different termination types for a BQ table load operation
 * 
 * @author Eyal Schneider
 */
enum LoadTerminationType {
  SUCCESS, 
  FAILED_TIMEOUT, // Timeout while waiting for the load to complete
  FAILED_INVALID_REQUEST, // The details of the load request are illegal
  FAILED_SERVER_ERROR, // Internal error in BQ Server
  FAILED_CLIENT_RESOURCES_LIMIT, // Client failed the request due to client side resources limit
  FAILED_OTHER, // Other error type, not specified here
  INTERNAL_ERROR // Indicates an unexpected error at our side (bug)
}
