package org.pipecraft.infra.bq;

import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Set;

/**
 * BQ table load configuration.
 * Serves both for local file loading and remote(cloud storage) file loading into BQ.
 * Differences:
 * 1) Source URIs - for remote load use "gs://..." paths only. For local load use full local file system paths only.
 * The URIs format determines whether this is a local load or a remote one.
 * 2) Wildcards - Both expect wildcards in the filename part of the file path only. Both support '*' wildcard, but only
 * local load supports multiple occurrences of '*' and also supports '?'.
 * 3) Parallelism - Note that local load is serial and isn't as efficient as remote load, so for large data prefer using remote load.
 * 4) Compression - remote load supports only gzip. Local load also supports zstd. Files must have the proper suffix to be identified correctly.
 * 
 * Immutable.
 *
 * @author Eyal Schneider
 */
public class TableLoadConfig {

  public enum LoadFormat {
    CSV, NEWLINE_DELIMITED_JSON, AVRO
  }

  private final Set<String> sourceURIs;
  private final TableId destinationTableReference;
  private final LocalDate destinationTablePartition;
  private final LoadFormat loadFormat;
  private final String csvFieldDelimiter;
  private final boolean csvHasHeader;
  private final Schema tableSchema;
  private final Integer destinationTableExpirationHs;
  private final WriteDisposition writeDisposition;
  private final CreateDisposition createDisposition;
  private final boolean allowJaggedRows;
  private final Long timeoutMillis;
  private final Set<String> clusteringFields;

  /**
   * Private constructor
   * 
   * @param builder The builder to copy the setting from
   */
  private TableLoadConfig(Builder builder) {
    this.sourceURIs = builder.getSourceURIs();
    this.destinationTableReference = builder.getDestinationTableReference();
    this.destinationTablePartition = builder.getDestinationTablePartition();
    this.loadFormat = builder.getLoadFormat();
    this.csvFieldDelimiter = builder.getCSVFieldDelimiter();
    this.csvHasHeader = builder.getCSVHasHeader();
    this.tableSchema = builder.getTableSchema();
    this.destinationTableExpirationHs = builder.getDestinationTableExpirationHs();
    this.writeDisposition = builder.getWriteDisposition();
    this.createDisposition = builder.getCreateDisposition();
    this.allowJaggedRows = builder.getAllowJaggedRows();
    this.timeoutMillis = builder.getTimeoutMs();
    this.clusteringFields = builder.getClusteringFields();
  }

  /**
   * @param sourceURIs the full paths to the source data.
   * Each URI should be fully qualified, and may contain one wildcard ('*')
   * in the file name part of the path.
   * When loading local files, all URIs should have the form of a full local file system path. For remote loading they should
   * all be valid cloud storage paths.
   * @param destinationTableReference The details of the table to load data into.
   * In case of a partitioned table, use setPartition(..) to set the partition to load into.
   * @return A builder initialized with the given sources and target, having default
   * values in the other fields.
   */
  public static Builder newBuilder(Set<String> sourceURIs, TableId destinationTableReference) {
    return new Builder(sourceURIs, destinationTableReference);
  }

  /**
   * @param sourceURI the full path to the source file.
   * The URI should be fully qualified, and may contain one wildcard ('*')
   * in the file name part of the path.
   * When loading a local file, the URI should have the form of a full local file system path. For remote loading it should
   * be a valid cloud storage paths.
   * @param destinationTableReference The details of the table to load data into
   * @return A builder initialized with the given source and target, having default
   * values in the other fields.
   */
  public static Builder newBuilder(String sourceURI, TableId destinationTableReference) {
    return new Builder(sourceURI, destinationTableReference);
  }

  /**
   * @return the full paths to the source data.
   * Each URI should be fully qualified, and may contain one wildcard ('*')
   * in the file name part of the path.
   * When loading local files, all URIs should have the form of a full local file system path. For remote loading they should
   * all be valid cloud storage paths.
   */
  public Set<String> getSourceURIs() {
    return sourceURIs;
  }

  /**
   * @return true if and only if this configuration is for a remote (cloud storage) file loading into BQ.
   * This is determined by inspecting the form of the source URIs.
   */
  public boolean isRemoteLoad() {
    return sourceURIs.isEmpty() || sourceURIs.iterator().next().startsWith("gs://");
  }
  
  /**
   * @return the destination table reference
   */
  public TableId getDestinationTableReference() {
    return destinationTableReference;
  }

  /**
   * @return The destination table partition to write to, as a date.
   * Should be specified only for partitioned tables.
   */
  public LocalDate getDestinationTablePartition() {
    return destinationTablePartition;
  }

  /**
   * @return the format of the input file.
   * Default is CSV.
   */
  public LoadFormat getLoadFormat() {
    return loadFormat;
  }

  /**
   * @return the input file field delimiter.
   * Applies to CSV format only. Default is ",".
   */
  public String getCsvFieldDelimiter() {
    return csvFieldDelimiter;
  }

  /**
   * Relevant for CSV format only.
   * @return True if and only if the csv file has a header line to skip. Default is true.
   */
  public boolean getCSVHasHeader() {
    return csvHasHeader;
  }

  /**
   * @return the destination table schema.
   * The schema can be omitted if the destination table already exists.
   * If specified, it can serve for adding columns dynamically.
   */
  public Schema getTableSchema() {
    return tableSchema;
  }

  /**
   * @return the destination table expiration in hours. 
   * Null means no expiration.
   */
  public Integer getDestinationTableExpirationHs() {
    return destinationTableExpirationHs;
  }

  /**
   * @return The table creation mode. 
   * Defines how the command deals with a situation where the table
   * to load into already exists.
   * Default is CREATE_IF_NEEDED.
   */
  public CreateDisposition getCreateDisposition() {
    return createDisposition;
  }
  
  /**
   * @return The mode defining how the command deals with 
   * existing rows in the target table.
   * Default is WRITE_APPEND.
   */
  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  /**
   * @return true if and only if jagged rows are allowed.
   * Jagged rows are rows that are missing optional columns (trailing columns only).
   * When true, the missing values are treated as nulls. When false, missing values are considered an error.
   * Default is false.
   */
  public boolean getAllowJaggedRows() {
    return allowJaggedRows;
  }

  /**
   * @return The set of names of table fields defined as clustering fields.
   * Mandatory when the table has clustering fields. 
   */
  public Set<String> getClusteringFields() {
    return clusteringFields;
  }

  /**
   * @return The load execution timeout, in milliseconds.
   * Null means no timeout (default value).
   * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
   * (The export level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
   */
  public Long getTimeoutMs() {
    return timeoutMillis;
  }

  @Override
  public String toString() {
    return "TableLoadConfig [sourceURIs=" + sourceURIs + ", destinationTableReference="
        + destinationTableReference + ", destinationTablePartition=" + destinationTablePartition
        + ", loadFormat=" + loadFormat + ", csvFieldDelimiter=" + csvFieldDelimiter
        + ", csvHasHeader=" + csvHasHeader + ", tableSchema=" + tableSchema
        + ", destinationTableExpirationHs=" + destinationTableExpirationHs + ", writeDisposition="
        + writeDisposition + ", createDisposition=" + createDisposition + ", allowJaggedRows="
        + allowJaggedRows + ", timeoutMillis=" + timeoutMillis + ", clusteringFields="
        + clusteringFields + "]";
  }

  public static class Builder {
    private final Set<String> sourceURIs;
    private final TableId destinationTableReference;
    private LocalDate destinationTablePartition;
    private LoadFormat loadFormat = LoadFormat.CSV;
    private String csvFieldDelimiter = ",";
    private boolean csvHasHeader = true;
    private Schema tableSchema;
    private Integer destinationTableExpirationHs;
    private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND; // Big Query's default write disposition for load action is append
    private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED; // Big Query's default create disposition
    private boolean allowJaggedRows;
    private Long timeoutMillis;
    private Set<String> clusteringFields;
    

    /**
     * Private constructor
     * 
     * @param sourceURIs the full paths to the source data.
     * Each URI should be fully qualified, and may contain one wildcard ('*')
     * in the file name part of the path.
     * When loading local files, all URIs should have the form of a full local file system path. For remote loading they should
     * all be valid cloud storage paths.
     * @param destinationTableReference The details of the table to load data into
     */
    private Builder(Set<String> sourceURIs, TableId destinationTableReference) {
      this.sourceURIs = sourceURIs;
      this.destinationTableReference = destinationTableReference;
    }

    /**
     * Private constructor
     * 
     * @param sourceURI the full path to the source file.
     * The URI should be fully qualified, and may contain one wildcard ('*')
     * in the file name part of the path.
     * When loading a local file, the URI should have the form of a full local file system path. For remote loading it should
     * be a valid cloud storage paths.
     * @param destinationTableReference The details of the table to load data into
     */
    private Builder(String sourceURI, TableId destinationTableReference) {
      this(Collections.singleton(sourceURI), destinationTableReference);
    }

    /**
     * @return the full paths to the source data.
     * Each URI should be fully qualified, and may contain one wildcard ('*')
     * in the file name part of the path.
     * When loading local files, all URIs should have the form of a full local file system path. For remote loading they should
     * all be valid cloud storage paths.
     */
    public Set<String> getSourceURIs() {
      return sourceURIs;
    }

    /**
     * @return the destination table reference
     */
    public TableId getDestinationTableReference() {
      return destinationTableReference;
    }

    /**
     * @param partition The destination table partition to write to, as a date.
     * Should be specified only for partitioned tables.
     */
    public Builder setDestinationTablePartition(LocalDate partition) {
      this.destinationTablePartition = partition;
      return this;
    }

    /**
     * @return The destination table partition to write to, as a date.
     * Should be specified only for partitioned tables.
     */
    public LocalDate getDestinationTablePartition() {
      return destinationTablePartition;
    }

    /**
     * @param loadFormat the format of the input file.
     * Default is CSV.
     * @return This builder object
     */
    public Builder setLoadFormat(LoadFormat loadFormat) {
      this.loadFormat = loadFormat;
      return this;
    }

    /**
     * @return the format of the input file.
     * Default is CSV.
     */
    public LoadFormat getLoadFormat() {
      return loadFormat;
    }

    /**
     * Applies to CSV format only.
     * @param csvFieldDelimiter the input file field delimiter.
     * Default is ",".
     * @return This builder object
     */
    public Builder setCSVFieldDelimiter(String csvFieldDelimiter) {
      this.csvFieldDelimiter = csvFieldDelimiter;
      return this;
    }

    /**
     * @return the input file field delimiter.
     * Applies to CSV format only. Default is ",".
     */
    public String getCSVFieldDelimiter() {
      return csvFieldDelimiter;
    }

    /**
     * Relevant for CSV format only.
     * @param csvHasHeader True if and only if the csv file has a header line to skip. Default is true.
     */
    public Builder setCSVHasHeader(boolean csvHasHeader) {
      this.csvHasHeader = csvHasHeader;
      return this;
    }

    /**
     * Relevant for CSV format only.
     * @return True if and only if the csv file has a header line to skip. Default is true.
     */
    public boolean getCSVHasHeader() {
      return csvHasHeader;
    }

    /**
     * @param tableSchema destination table schema.
     * The schema can be null if the destination table already exists.
     * If specified, it can serve for adding columns dynamically.
     * @return This builder object
     */
    public Builder setTableSchema(Schema tableSchema) {
      this.tableSchema = tableSchema;
      return this;
    }

    /**
     * @return the destination table schema.
     * The schema can be omitted if the destination table already exists.
     * If specified, it can serve for adding columns dynamically.
     */
    public Schema getTableSchema() {
      return tableSchema;
    }

    /**
     * @param destinationTableExpirationHs the destination table expiration in hours. 
     * Null means no expiration.
     * @return this builder object
     */
    public Builder setDestinationTableExpirationHs(Integer destinationTableExpirationHs) {
      this.destinationTableExpirationHs = destinationTableExpirationHs;
      return this;
    }

    /**
     * @return the destination table expiration in hours. 
     * Null means no expiration.
     */
    public Integer getDestinationTableExpirationHs() {
      return destinationTableExpirationHs;
    }

    /**
     * @param createDisposition The table creation mode. 
     * Defines how the command deals with a situation where the table
     * to load into already exists.
     * Default is CREATE_IF_NEEDED.
     * @return This builder object
     */
    public Builder setCreateDisposition(CreateDisposition createDisposition) {
      this.createDisposition = createDisposition;
      return this;
    }

    /**
     * @return The table creation mode. 
     * Defines how the command deals with a situation where the table
     * to load into already exists.
     * Default is CREATE_IF_NEEDED.
     */
    public CreateDisposition getCreateDisposition() {
      return createDisposition;
    }
    
    /**
     * @param writeDisposition The mode defining how the command deals with 
     * existing rows in the target table.
     * Default is WRITE_APPEND.
     * @return This builder object
     */
    public Builder setWriteDisposition(WriteDisposition writeDisposition) {
      this.writeDisposition = writeDisposition;
      return this;
    }

    /**
     * @return The mode defining how the command deals with 
     * existing rows in the target table.
     * Default is WRITE_APPEND.
     */
    public WriteDisposition getWriteDisposition() {
      return writeDisposition;
    }

    /**
     * @param allowJaggedRows true if and only if jagged rows are allowed.
     * Jagged rows are rows that are missing optional columns (trailing columns only).
     * When true, the missing values are treated as nulls. When false, missing values are considered an error.
     * Default is false.
     *
     * @return This builder object
     */
    public Builder setAllowJaggedRows(boolean allowJaggedRows) {
      this.allowJaggedRows = allowJaggedRows;
      return this;
    }

    /**
     * @return true if and only if jagged rows are allowed.
     * Jagged rows are rows that are missing optional columns (trailing columns only).
     * When true, the missing values are treated as nulls. When false, missing values are considered an error.
     * Default is false.
     */
    public boolean getAllowJaggedRows() {
      return allowJaggedRows;
    }

    /**
     * @param clusteringFields The set of names of table fields defined as clustering fields.
     * Mandatory when the table has clustering fields. 
     */
    public Builder setClusteringFields(Set<String> clusteringFields) {
      this.clusteringFields = clusteringFields;
      return this;
    }

    /**
     * @return The set of names of table fields defined as clustering fields.
     * Mandatory when the table has clustering fields. 
     */
    public Set<String> getClusteringFields() {
      return clusteringFields;
    }

    /**
     * @param timeoutMs The load execution timeout to set, in milliseconds.
     * Must be positive.
     * Null means no timeout.
     * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
     * (The export level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
     * @return This builder object
     */
    public Builder setTimeoutMs(Long timeoutMs) {
      this.timeoutMillis = timeoutMs;
      return this;
    }

    /**
     * @return The load execution timeout, in milliseconds.
     * Null means no timeout (default value).
     * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
     * (The export level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
     */
    public Long getTimeoutMs() {
      return timeoutMillis;
    }
    
    /**
     * @return A new {@link TableLoadConfig} with the current settings.
     */
    public TableLoadConfig build() {
      return new TableLoadConfig(this);
    }
  }
}
