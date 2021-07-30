package org.pipecraft.infra.bq;

import com.google.cloud.bigquery.FieldValueList;
import org.pipecraft.infra.bq.exceptions.BQException;
import java.util.Iterator;

/**
 * A base interface for BQ queries with no returned result, or with a returned result that can be dropped.
 * Executed via {@link BigQueryConnector}
 *
 * @author Eyal Schneider
 * 
 */
public interface BQResultlessQuery extends BQQuery<Void, Void> {
  /**
   * @param row A result set row
   * @return The object the row is converted to
   */
  public default Void mapRow(FieldValueList row) {
    return null;
  }

  /**
   * @param it An iterator over transformed rows (i.e. rows processed with transformRow(..)). Memory efficient - results are streamed during the iteration.
   * @return The final result object, obtained by aggregating all result rows
   * @throws BQException In case that the results can't be aggregated
   */
  public default Void aggregate(Iterator<Void> it) throws BQException {
    return null;
  }
}
