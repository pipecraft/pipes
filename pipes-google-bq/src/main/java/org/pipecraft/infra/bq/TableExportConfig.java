package org.pipecraft.infra.bq;

import com.google.cloud.bigquery.TableId;
import java.util.Collections;
import java.util.Set;

/**
 * Settings for BQ table export operation.
 * An export consists of copying table's contents to GoogleStorage.
 * Immutable.
 *
 * @author Eyal Rubichi, Eyal Schneider
 */
public class TableExportConfig {
  public enum ExportFormat {
    CSV, NEWLINE_DELIMITED_JSON, AVRO
  }

  public enum Compression {
    NONE, GZIP
  }

  private final TableId sourceTableReference;
  private final Set<String> destinationURIs;
  private final ExportFormat exportFormat;
  private final Compression compression;
  private final char csvFieldDelimiter;
  private final boolean printHeader;
  private final Long timeoutMillis;

  /**
   * @param sourceTableReference reference to the table to export 
   * @param destinationURIs The set of fully-qualified Google Cloud Storage URIs where the table should be written to.
   * A URI may contain a wildcard ('*') in the file name part of the path, indicating that a sharded output is required.
   * BigQuery makes it mandatory for large tables.
   * @return A new builder initialized with the given source and destinations,
   * having default values for other fields.
   */
  public static Builder newBuilder(TableId sourceTableReference, Set<String> destinationURIs) {
    return new Builder(sourceTableReference, destinationURIs);
  }

  /**
   * @param sourceTableReference reference to the table to export 
   * @param destinationURI The fully-qualified Google Cloud Storage URI where the table should be written to.
   * A URI may contain a wildcard ('*') in the file name part of the path, indicating that a sharded output is required.
   * BigQuery makes it mandatory for large tables.
   * @return A new builder initialized with the given source and destination,
   * having default values for other fields.
   */
  public static Builder newBuilder(TableId sourceTableReference, String destinationURI) {
    return new Builder(sourceTableReference, destinationURI);
  }

  /**
   * Private constructor
   * @param builder The builder to get the setting from
   */
  private TableExportConfig(Builder builder) {
    this.exportFormat = builder.getExportFormat();
    this.compression = builder.getCompression();
    this.csvFieldDelimiter = builder.getCSVFieldDelimiter();
    this.printHeader = builder.isPrintHeader();
    this.sourceTableReference = builder.getSourceTableReference();
    this.destinationURIs = builder.getDestinationURIs();
    this.timeoutMillis = builder.getTimeoutMs();
  }
  
  /**
   * @return the format of the output file.
   * Default is CSV.
   */
  public ExportFormat getExportFormat() {
    return exportFormat;
  }

  /**
   * @return the compression of the output file/s.
   * Default is NONE.
   */
  public Compression getCompression() {
    return compression;
  }

  /**
   * @return the field delimiter in the output file/s.
   * Default is ','.
   * Relevant only for CSV format.
   */
  public char getCSVFieldDelimiter() {
    return csvFieldDelimiter;
  }

  /**
   * @return indicates whether the file should contain the column names.
   * Relevant only for CSV format.
   * Default is true.
   */
  public boolean isPrintHeader() {
    return printHeader;
  }

  /**
   * @return reference to the table to export
   */
  public TableId getSourceTableReference() {
    return sourceTableReference;
  }

  /**
   * @return The set of fully-qualified Google Cloud Storage URIs where the table should be written to.
   * A URI may contain a wildcard ('*') in the file name part of the path, indicating that a sharded output is required.
   * BigQuery makes it mandatory for large tables.
   */
  public Set<String> getDestinationURIs() {
    return destinationURIs;
  }

  /**
   * @return The export execution timeout, in milliseconds. Null means no timeout.
   * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
   * (The export level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
   */
  public Long getTimeoutMs() {
    return timeoutMillis;
  }

  @Override
  public String toString() {
    return "TableExportConfig [sourceTableReference=" + sourceTableReference + ", destinationURIs="
        + destinationURIs + ", exportFormat=" + exportFormat + ", compression=" + compression
        + ", csvFieldDelimiter=" + csvFieldDelimiter + ", printHeader=" + printHeader
        + ", timeoutMillis=" + timeoutMillis + "]";
  }

  // Used for building TableExportConfig instances
  public static class Builder {
    private final TableId sourceTableReference;
    private final Set<String> destinationURIs;
    private ExportFormat exportFormat = ExportFormat.CSV;
    private Compression compression = Compression.NONE;
    private char csvFieldDelimiter = ',';
    private boolean printHeader = true;
    private Long timeoutMillis;
    
    /**
     * Constructor
     * @param sourceTableReference reference to the table to export
     * @param destinationURIs A set of fully-qualified Google Cloud Storage URIs where the table should be exported to
     */
    public Builder(TableId sourceTableReference, Set<String> destinationURIs) {
      this.sourceTableReference = sourceTableReference;
      this.destinationURIs = destinationURIs;
    }

    /**
     * Constructor
     * @param sourceTableReference reference to the table to export
     * @param destinationURI The Google Cloud Storage URI where the table should be exported to
     */
    public Builder(TableId sourceTableReference, String destinationURI) {
      this(sourceTableReference, Collections.singleton(destinationURI));
    }

    /**
     * @param exportFormat the format of the output file. Default is CSV.
     * @return This builder object
     */
    public Builder setExportFormat(ExportFormat exportFormat) {
      this.exportFormat = exportFormat;
      return this;
    }

    /**
     * @return the format of the output file/s. Default is CSV.
     */
    public ExportFormat getExportFormat() {
      return exportFormat;
    }

    /**
     * @param compression the compression of the output file/s.
     * Default is NONE.
     * @return This builder object
     */
    public Builder setCompression(Compression compression) {
      this.compression = compression;
      return this;
    }

    /**
     * @return the compression of the output file/s. Default is NONE.
     */
    public Compression getCompression() {
      return compression;
    }

    /**
     * @param csvFieldDelimiter the field delimiter in the output file/s.
     * Default is ",".
     * Relevant only for CSV format.
     * @return This builder object
     */
    public Builder setFieldDelimiter(char csvFieldDelimiter) {
      this.csvFieldDelimiter = csvFieldDelimiter;
      return this;
    }

    /**
     * @return the field delimiter in the output file.
     * Default is ','.
     * Relevant only for CSV format.
     */
    public char getCSVFieldDelimiter() {
      return csvFieldDelimiter;
    }

    /**
     * @param printHeader indicates whether the file should contain the column names.
     * Relevant only for CSV.
     * Default is true.
     * @return This builder object
     */
    public Builder setPrintHeader(boolean printHeader) {
      this.printHeader = printHeader;
      return this;
    }

    /**
     * @return indicates whether the file should contain the column names.
     * Relevant only for CSV.
     * Default is true.
     */
    public boolean isPrintHeader() {
      return printHeader;
    }

    /**
     * @return reference to the table to export
     */
    public TableId getSourceTableReference() {
      return sourceTableReference;
    }

    /**
     * @return A set of fully-qualified Google Cloud Storage URIs where the extracted table should be written.
     * A URI may contain a wildcard ('*') in the file name part of the path, indicating that a sharded output is required.
     * BigQuery makes it mandatory for large tables.
     */
    public Set<String> getDestinationURIs() {
      return destinationURIs;
    }
    
    /**
     * @param timeoutMs The export execution timeout to set, in milliseconds.
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
     * @return The export execution timeout, in milliseconds.
     * Null means no timeout (default value).
     * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
     * (The export level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor.
     */
    public Long getTimeoutMs() {
      return timeoutMillis;
    }

    /**
     * @return A new {@link TableExportConfig} using the current settings
     */
    public TableExportConfig build() {
      return new TableExportConfig(this);
    }
  }
}
