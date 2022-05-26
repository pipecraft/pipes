package org.pipecraft.pipes.sync.source;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.concurrent.FailableSupplier;
import org.pipecraft.pipes.exceptions.JdbcPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe reading rows from a DB query result set, and converting them to some entity type
 * 
 * @param <T> The entity type
 *
 * @author Eyal Schneider
 */
public class JdbcQueryResultsPipe<T> implements Pipe<T> {
  private final FailableSupplier<Connection, SQLException> connectionSupplier;
  private final String query;
  private final FailableFunction<ResultSet, T, SQLException> rowMapper;
  private Connection connection;
  private Statement statement;
  private ResultSet resultSet;
  private T next;
  private volatile boolean done;
  
  
  /**
   * Constructor
   * 
   * @param connectionSupplier The DB connection supplier to use for getting a connection. Invoked only once.
   * @param query The query to execute. Must be a valid SELECT query.
   * @param rowMapper A mapper from {@link ResultSet} rows to the corresponding entity of type T. 
   * Must be compatible with the output column names of the supplied query.
   * Must not mutate the result set by iterating over it.
   */
  public JdbcQueryResultsPipe(FailableSupplier<Connection, SQLException> connectionSupplier, String query, FailableFunction<ResultSet, T , SQLException> rowMapper) {
    this.connectionSupplier = connectionSupplier;
    this.query = query;
    this.rowMapper = rowMapper;
  }
  
  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public T peek() throws PipeException {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      connection = connectionSupplier.get();
      statement = connection.createStatement();
      resultSet = statement.executeQuery(query);
      prepareNext();
    } catch (SQLException e) {
      SQLException e2 = closeResources(e);
      throw new JdbcPipeException(e2);
    }
  }

  @Override
  public float getProgress() {
    //TODO(EyalS): Real progress tracking requires adding inefficiency, by either using rs.last()+rs.getRow(), or running a SELECT count(*) from {original query} prior to actual query.
    // Consider adding this as an optional (default false) option.
    if (done) {
      return 1.0f;
    }
    return 0;
  }

  private void prepareNext() throws PipeException {
    try {
      if (!done && resultSet.next()) {
        next = rowMapper.apply(resultSet);
      } else {
        next = null;
        done = true;
      }
    } catch (SQLException e) {
      SQLException e2 = closeResources(e);
      throw new JdbcPipeException(e2);
    }
  }

  @Override
  public void close() throws IOException {
    SQLException e = closeResources(null);
    if (e != null) {
      throw new IOException(e);
    }
  }

  private SQLException closeResources(SQLException source) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        if (source != null) {
          source.addSuppressed(e);
        } else {
          source = e;
        }
      }
    }

    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        if (source != null) {
          source.addSuppressed(e);
        } else {
          source = e;
        }
      }
    }

    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        if (source != null) {
          source.addSuppressed(e);
        } else {
          source = e;
        }
      }
    }

    return source;
  }
}
