package org.pipecraft.infra.bq;

import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.TableId;
import org.pipecraft.infra.bq.exceptions.ClientTooManyRowsBQException;

/**
 * BQ query execution settings.
 * Immutable. 
 * Use the builder to create or create a copy of existing config.
 *
 * @author Eyal Schneider
 */
public class QueryExecutionConfig {
  private final Long timeoutMillis;
  private final boolean disableCache;
  private final int maxRowsPerPage;
  private final Long maxResults;
  private final TableId destinationTableReference;
  private final Integer destinationTableExpirationHs;
  private final WriteDisposition writeDisposition;
  private final Priority priority;
  
  /**
   * Private constructor to be invoked by the builder
   * 
   * @param builder The builder to copy all fields from
   */
  private QueryExecutionConfig(Builder builder) {
    this.timeoutMillis = builder.getTimeoutMs();
    this.disableCache = builder.isCacheDisabled();
    this.maxRowsPerPage = builder.getMaxRowsPerPage();
    this.maxResults = builder.getMaxResults();
    this.destinationTableReference = builder.getDestinationTableReference();
    this.writeDisposition = builder.getWriteDisposition();
    this.priority = builder.getPriority();
    this.destinationTableExpirationHs = builder.getDestinationTableExpirationHs();
  }
  
  /**
   * @return A new builder with defaults
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @param config The configuration to copy all settings from
   * @return A new builder initialized with the given config
   */
  public static Builder newBuilder(QueryExecutionConfig config) {
    return new Builder(config);
  }

  /**
   * @return A new builder initialized with the settings in this config object.
   * Allows selectively changing the settings.
   */
  public Builder toBuilder() {
    return newBuilder(this);
  }
  
  /**
   * @return The query execution timeout, in milliseconds. Null means no timeout.
   * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
   * (The query level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
   */
  public Long getTimeoutMs() {
    return timeoutMillis;
  }

  /**
   * @return true for avoiding usage of cache, false for allowing it where relevant
   */
  public boolean isCacheDisabled() {
    return disableCache;
  }

  /**
   * @return The maximum number of rows to use per page. 
   * BQ returns result rows in chunks called "pages".
   */
  public int getMaxRowsPerPage() {
    return maxRowsPerPage;
  }

  /**
   * @return The maximum number of rows to return for a single query. Null means no limit.
   * This is also referred to as the "soft limit".
   * A query which returns more results will trigger the {@link ClientTooManyRowsBQException}.
   */
  public Long getMaxResults() {
    return maxResults;
  }

  /**
   * @return The identity of the BQ table to write results to.
   * Null means no specific table (default).
   */
  public TableId getDestinationTableReference() {
    return destinationTableReference;
  }

  /**
   * Relevant only when destination table is set.
   * @return The action that occurs if the destination table already exists. The following
   * values are supported: WRITE_EMPTY, WRITE_TRUNCATE and WRITE_APPEND. The default value is WRITE_EMPTY.
   * See {@link WriteDisposition} documentation.
   */
  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  /**
   * Returns the query priority. Possible values include INTERACTIVE and BATCH.
   *
   * @return The query job priority. Possible values include INTERACTIVE and BATCH.
   */
  public Priority getPriority() {
    return priority;
  }

  /**
   * Relevant only when destination table is set.
   * @return the destination table expiration time, in hours. 
   * Null means no expiration.
   */
  public Integer getDestinationTableExpirationHs() {
    return destinationTableExpirationHs;
  }
  
  // Used for building QueryExecutionConfig instances
  public static class Builder {
    private Long timeoutMillis; // Default null, meaning no timeout.
    private boolean disableCache; // Default false, meaning that server side cache is enabled
    private int maxRowsPerPage = 10_000; // Default page size is 10,000 rows.
    private Long maxResults; // Default is null, meaning no soft limit
    private TableId destinationTableReference; // Default is null - no destination table.
    private Integer destinationTableExpirationHs;
    private WriteDisposition writeDisposition = WriteDisposition.WRITE_EMPTY; // Same as Big Query's default
    private Priority priority = Priority.INTERACTIVE; // By default we want queries to be served ASAP

    /*
     * Empty constructor for a new builder with defaults
     */
    private Builder() {
    }

    /*
     * Constructor for a new builder based on existing instance
     */
    private Builder(QueryExecutionConfig config) {
      this.timeoutMillis = config.getTimeoutMs();
      this.disableCache = config.isCacheDisabled();
      this.maxRowsPerPage = config.getMaxRowsPerPage();
      this.maxResults = config.getMaxResults();
      this.destinationTableReference = config.getDestinationTableReference();
      this.writeDisposition = config.getWriteDisposition();
      this.priority = config.getPriority();
      this.destinationTableExpirationHs = config.getDestinationTableExpirationHs();
    }

    /**
     * @param timeoutMs The query execution timeout to set, in milliseconds.
     * Must be positive.
     * Null means no timeout.
     * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
     * (The query level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
     * @return This builder object
     */
    public Builder setTimeoutMs(Long timeoutMs) {
      this.timeoutMillis = timeoutMs;
      return this;
    }

    /**
     * @return The query execution timeout, in milliseconds.
     * Null means no timeout (default value).
     * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
     * (The query level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
     */
    public Long getTimeoutMs() {
      return timeoutMillis;
    }

    /**
     * @param disableCache true for avoiding usage of cache, false for allowing it where relevant (default).
     * @return This builder object
     */
    public Builder setDisableCache(boolean disableCache) {
      this.disableCache = disableCache;
      return this;
    }

    /**
     * @return true for avoiding usage of cache, false for allowing it where relevant
     */
    public boolean isCacheDisabled() {
      return disableCache;
    }

    /**
     * @param maxRowsPerPage The maximum number of rows to use per page. 
     * BQ returns result rows in chunks called "pages". 
     * This number must be positive. Default value is 10,000.
     * @return This builder object
     */
    public Builder setMaxRowsPerPage(int maxRowsPerPage) {
      this.maxRowsPerPage = maxRowsPerPage;
      return this;
    }

    /**
     * @return The maximum number of rows to use per page. 
     * BQ returns result rows in chunks called "pages". 
     * This number must be positive. Default value is 10,000.
     */
    public int getMaxRowsPerPage() {
      return maxRowsPerPage;
    }

    /**
     * @param maxResults The maximum number of rows to return for a single query. Null means no limit.
     * This is also referred to as the "soft limit".
     * A query which returns more results will trigger the {@link TooManyRowsException}.
     * @return This builder object
     */
    public Builder setMaxResults(Long maxResults) {
      this.maxResults = maxResults;
      return this;
    }

    /**
     * @return The maximum number of rows to return for a single query. Null means no limit.
     * This is also referred to as the "soft limit".
     * A query which returns more results will trigger the {@link TooManyRowsException}.
     */
    public Long getMaxResults() {
      return maxResults;
    }

    /**
     * @param tableConfig The identity of the BQ table to write results to.
     * Null means no specific table (default).
     * @return This builder object
     */
    public Builder setTableDestinationReference(TableId tableConfig) {
      destinationTableReference = tableConfig;
      return this;
    }

    /**
     * @return The identity of the BQ table to write results to.
     * Null means no specific table (default).
     */
    public TableId getDestinationTableReference() {
      return destinationTableReference;
    }

    /**
     * Relevant only when destination table is set.
     * Specifies the action that occurs if the destination table already exists. The following
     * values are supported: WRITE_EMPTY, WRITE_TRUNCATE and WRITE_APPEND. The default value is WRITE_EMPTY.
     * See {@link WriteDisposition} documentation.
     * 
     * @param action The disposition to set. Must not be empty.
     * @return This builder object
     */
    public Builder setWriteDisposition(WriteDisposition action) {
      writeDisposition = action;
      return this;
    }

    /**
     * Relevant only when destination table is set.
     * 
     * @return the action that occurs if the destination table already exists. The following
     * values are supported: WRITE_EMPTY, WRITE_TRUNCATE and WRITE_APPEND. The default value is WRITE_EMPTY.
     * See {@link WriteDisposition} documentation.
     */
    public WriteDisposition getWriteDisposition() {
      return writeDisposition;
    }

    /**
     * Specifies a priority for the query. Possible values include INTERACTIVE and BATCH.
     * The default value is INTERACTIVE.
     *
     * @return This builder object
     */
    public Builder setPriority(Priority priority) {
      this.priority = priority;
      return this;
    }

    /**
     * @return The query job priority. Possible values include INTERACTIVE and BATCH.
     * Default is INTERACTIVE.
     */
    public Priority getPriority() {
      return priority;
    }

    /**
     * Relevant only when destination table is set.
     * @return the destination table expiration time, in hours. 
     * Null means no expiration(default).
     */
    public Integer getDestinationTableExpirationHs() {
      return destinationTableExpirationHs;
    }

    /**
     * Relevant only when destination table is set.
     * @param destinationTableExpirationHs the destination table expiration time, in hours. 
     * Null means no expiration(default).
     * @return this builder object
     */
    public Builder setDestinationTableExpirationHs(Integer destinationTableExpirationHs) {
      this.destinationTableExpirationHs = destinationTableExpirationHs;
      return this;
    }

    /**
     * @return A new {@link QueryExecutionConfig} based on the current builder state
     */
    public QueryExecutionConfig build() {
      return new QueryExecutionConfig(this);
    }
  }
}
