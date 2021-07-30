package org.pipecraft.infra.bq;

/**
 * A base interface for DML (Data Manipulation Language) BQ queries.
 * DML queries have no returned data, and use by default standard SQL syntax rather than legacy syntax.
 * Executed via {@link BigQueryConnector}
 *
 * @author Eyal Schneider
 * 
 */
public interface BQDMLQuery extends BQResultlessQuery {
}
