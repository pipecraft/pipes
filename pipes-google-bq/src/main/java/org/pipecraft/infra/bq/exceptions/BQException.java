package org.pipecraft.infra.bq.exceptions;

import com.google.cloud.bigquery.QueryParameterValue;
import org.pipecraft.infra.bq.BQQuery;
import java.util.Map;

/**
 * The ancestor class of all checked BQ exceptions
 * Descendants are either {@link TransientBQException} or {@link NonTransientBQException}.
 *
 * @author Eyal Schneider
 */
@SuppressWarnings("serial")
public abstract class BQException extends Exception {

  /**
   * Constructor
   *
   * @param msg The error message
   * @param cause The cause of this exception
   */
  public BQException(String msg, Throwable cause) {
    this(msg, cause, null);
  }

  /**
   * Constructor
   *
   * @param msg The error message
   */
  public BQException(String msg) {
    this(msg, (BQQuery<?, ?>) null);
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   * 
   * @param cause The cause of this exception
   * @param query The failed query.
   */
  public BQException(Throwable cause, BQQuery<?, ?> query) {
    super(getQueryExpression(query), cause);
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   * 
   * @param msg The error message
   * @param query The failed query
   */
  public BQException(String msg, BQQuery<?, ?> query) {
    super(msg + "\n" + getQueryExpression(query));
  }

  /**
   * Constructor
   *
   * To be used for failed query requests only.
   *
   * @param msg The error message
   * @param cause The cause of this exception
   * @param query The failed query
   */
  public BQException(String msg, Throwable cause, BQQuery<?, ?> query) {
    super(msg + "\n" + getQueryExpression(query), cause);
  }

  private static String getQueryExpression(BQQuery<?, ?> query) {
    if (query == null) {
      return "Query: N/A";
    }

    StringBuilder builder = new StringBuilder("Query: ")
        .append(query.getSQL());

    Map<String, QueryParameterValue> queryParameters = query.getQueryParameters();
    if (queryParameters != null) {
      builder.append("\nParameters: ");
      for (Map.Entry<String, QueryParameterValue> paramPair : queryParameters.entrySet()) {
        QueryParameterValue paramValue = paramPair.getValue();
        String value = paramValue.getValue() != null ? paramValue.getValue() : String.valueOf(paramValue.getArrayValues());
        builder
            .append(paramPair.getKey())
            .append(": ")
            .append(value)
            .append("\n");
      }
    }

    return builder.toString();
  }
}
