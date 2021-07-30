package org.pipecraft.infra.bq;

/**
 * Class to contain BQ execution data.
 * Immutable
 *
 * @author Reem Khalaila
 *
 */
public class BQQueryExecutionSummary {
  // execution time
  private final long startTime;
  // Job id generated for the query
  private final String jobId;
  // Total size processed in bytes
  private final Long bytes;
  // Is the results took from cache
  private final Boolean isCacheHit;
  // Total row in the response
  private final Long totalRows;
  // The class name that call for BQ execute
  private final String qClassName;
  // The sql query executed
  private final String sql;
  // Duration of execution
  private final float durationSec;
  private final boolean isSuccessful;

  /**
   * Constructor
   * 
   * @param startTime The query start time, as millis since epoch
   * @param jobId The query's job id
   * @param bytes Total number of bytes processed
   * @param isCacheHit Indicates whether BQ cache has been used
   * @param totalRows Total number of rows in the query results
   * @param qClassName The query class name (some descendant of {@link BQQuery}). Not fully qualified name - only the class name is used here.
   * @param sql The executed query, as a legal query (no placeholders)
   * @param durationSec Total query execution time, in seconds
   * @param isSuccessful Indicates whether the query completed successfully
   */
  public BQQueryExecutionSummary(long startTimeMs, String jobId, Long bytes,
      Boolean isCacheHit, Long totalRows, String qClassName, String sql, float durationSec,
      boolean isSuccessful) {
    this.startTime = startTimeMs;
    this.jobId = jobId;
    this.bytes = bytes;
    this.isCacheHit = isCacheHit;
    this.totalRows = totalRows;
    this.qClassName = qClassName;
    this.sql = sql;
    this.durationSec = durationSec;
    this.isSuccessful = isSuccessful;
  }

  /**
   * @return The query start time, as millis since epoch
   */
  public long getStartTimeMs() {
    return startTime;
  }

  /**
   * @return The query's job id
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @return Total number of bytes processed
   */
  public Long getProcessedBytes() {
    return bytes;
  }

  /**
   * @return Indicates whether BQ cache has been used
   */
  public Boolean getIsCacheHit() {
    return isCacheHit;
  }

  /**
   * @return Total number of rows in the query results
   */
  public Long getTotalRows() {
    return totalRows;
  }

  /**
   * @return The query class name (some descendant of {@link BQQuery}). Not fully qualified name - only the class name is used here.
   */
  public String getQueryClassName() {
    return qClassName;
  }

  /**
   * @return The executed query, as a legal query (no placeholders)
   */
  public String getSql() {
    return sql;
  }

  /**
   * @return Total query execution time, in seconds
   */
  public float getDurationSec() {
    return durationSec;
  }

  /**
   * @return Indicates whether the query completed successfully
   */
  public boolean getIsSuccessful() {
    return isSuccessful;
  }

  @Override
  public String toString() {
    return "BQQueryExecutionSummary [startTime=" + startTime + ", jobId=" + jobId + ", bytes="
        + bytes + ", isCacheHit=" + isCacheHit + ", totalRows=" + totalRows + ", qClassName="
        + qClassName + ", sql=" + sql + ", durationSec=" + durationSec + ", isSuccessful="
        + isSuccessful + "]";
  }
}
