package org.pipecraft.infra.bq;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.collect.Iterators;
import org.pipecraft.infra.bq.exceptions.BQException;
import java.util.Iterator;
import java.util.Map;

/**
 * An BQ query to be executed using {@link BigQueryConnector}
 *
 * @param <R> The type a row is transformed to
 * @param <F> The final result type (Optional, use {@link Void} if not relevant)
 *
 * @author Eyal Schneider
 *
 */
public interface BQQuery<R, F> {
  /**
   * @return The BigQuery SQL to execute
   */
  String getSQL();

  /**
   * @param row A result set row
   * @return The object the row is converted to
   */
  R mapRow(FieldValueList row);

  /**
   * @param it An iterator over transformed rows (i.e. rows processed with transformRow(..)). Memory efficient - results are streamed during the iteration.
   * @return The final result object, obtained by aggregating all result rows
   * @throws BQException In case that the results can't be aggregated
   */
  F aggregate(Iterator<R> it) throws BQException;

  /**
   * @param it An iterator on raw results
   * @return The final result, obtained by mapping individual rows and then aggregating all
   * @throws BQException
   */
  default F mapAndAggregate(Iterator<FieldValueList> it) throws BQException {
    Iterator<R> it2 = Iterators.transform(it, this::mapRow);
    return aggregate(it2);
  }

  /**
   * @return true if this is a legacy sql query (default=false)
   */
  default boolean isLegacySQL() {
    return false;
  }
  
  /**
   * Used for supporting named parameters. The default implementation disables named parameters. Override this method to support it.
   * For every parameter used in the query (e.g. `@param`), there must be one entry with key `param`.
   *
   * @return the parameter names mapped to their values. Null means that the query doesn't contain any named parameters (default).
   */
  default Map<String, QueryParameterValue> getQueryParameters() {
    return null;
  }
}
