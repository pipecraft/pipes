package org.pipecraft.infra.bq;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Create or replaces an existing temp table in bq, with a given expiration duration.
 * 
 * @author Eyal Schneider
 *
 */
public class CreateTableDMLQuery implements BQDMLQuery {
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private final String dataSetId;
  private final String tableName;
  private final String schema;
  private final Duration expirationDuration;

  /**
   * @param dataSetId a BQ dataset where to place the table.
   * @param tableName The name of the table to create
   * @param schema The table schema, using legal BQ syntax (e.g. "(a STRING, b INT64)")
   * @param expiration The time till table expiration. Measured from query execution (i.e. call to getSQL()). Use null for no expiration.
   */
  public CreateTableDMLQuery(String dataSetId, String tableName, String schema, Duration expiration) {
    this.dataSetId = dataSetId;
    this.tableName = tableName;
    this.schema = schema;
    this.expirationDuration = expiration; 
  }

  @Override
  public String getSQL() {
    String expirationExpression = (expirationDuration == null) ? 
        "" :
        " OPTIONS (expiration_timestamp=TIMESTAMP \"" + ZonedDateTime.now(UTC).plus(expirationDuration).format(DATE_TIME_FORMATTER) + " UTC\")";
    return "CREATE OR REPLACE TABLE " + dataSetId + "." + tableName + " " + schema + expirationExpression;
  }
}
