package org.pipecraft.infra.bq;

import com.google.api.client.util.Clock;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobField;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;
import com.google.cloud.bigquery.LoadConfiguration;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Builder;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.pipecraft.infra.bq.TableLoadConfig.LoadFormat;
import org.pipecraft.infra.bq.exceptions.BQException;
import org.pipecraft.infra.bq.exceptions.ClientTooManyRowsBQException;
import org.pipecraft.infra.bq.exceptions.InvalidExportBQException;
import org.pipecraft.infra.bq.exceptions.InvalidQueryBQException;
import org.pipecraft.infra.bq.exceptions.ServerResourcesBQException;
import org.pipecraft.infra.bq.exceptions.TimeoutBQException;
import org.pipecraft.infra.bq.exceptions.ClientResourcesBQException;
import org.pipecraft.infra.bq.exceptions.InvalidTableLoadBQException;
import org.pipecraft.infra.bq.exceptions.NonTransientBQException;
import org.pipecraft.infra.bq.exceptions.QueryResultBrokenException;
import org.pipecraft.infra.bq.exceptions.ServerTooManyRowsBQException;
import org.pipecraft.infra.bq.exceptions.UnavailableBQException;
import org.pipecraft.infra.bq.exceptions.IOTransientBQException;
import org.pipecraft.infra.bq.exceptions.TransientBQException;
import org.pipecraft.infra.concurrent.AbstractCheckedFuture;
import org.pipecraft.infra.concurrent.CheckedFuture;
import org.pipecraft.infra.concurrent.CheckedFutureTransformer;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.monitoring.JsonMonitorable;
import org.pipecraft.infra.monitoring.JsonMonitorableWrapper;
import org.pipecraft.infra.monitoring.collectors.ActionStatsCollector;
import org.pipecraft.infra.monitoring.collectors.NonBlockingActionStatsCollector;
import org.pipecraft.infra.storage.PathUtils;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.Channels;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import net.minidev.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for interacting with Google's BigQuery.
 * Supports:
 * 1) Initialization with environment credentials (no option to set credentials explicitly yet)
 * 2) Select and DML queries (through {@link BQQuery} and {@link BQDMLQuery})
 * 3) Async query executions
 * 4) Writing query results to specific BQ tables
 * 5) Exporting results to google storage, asynchronously
 * 6) Load local/google-storage files into BQ tables, asynchronously
 * 7) Monitoring of all operations
 * 8) Allows limiting the number of concurrent BQ operations, using constructor parameters
 * 
 * @author Eyal Schneider
 */
public class BigQueryConnector implements JsonMonitorable {
  private static final Logger log = LoggerFactory.getLogger(BigQueryConnector.class);
  private static final DateTimeFormatter PARTITION_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
  
  private final ConcurrentHashMap<Class<? extends BQQuery<?, ?>>, ActionStatsCollector<QueryTerminationType>> stats =
      new ConcurrentHashMap<>();
  private final String projectId;
  private final BigQuery bigquery;
  private final Consumer<BQQueryExecutionSummary> observer;
  private final QueryExecutionConfig defaultExecConfig;
  private final ExecutorService providedExecutor;
  private final ListeningExecutorService ex;
  private final ActionStatsCollector<ExportTerminationType> exportStats = 
      new NonBlockingActionStatsCollector<>(ExportTerminationType.class);
  private final ActionStatsCollector<LoadTerminationType> loadStats = 
      new NonBlockingActionStatsCollector<>(LoadTerminationType.class);
  
  /**
   * Constructor
   *
   * @param projectId The project that this instance is bound to. All actions will be performed in the scope of this project.
   * @param connTimeoutMs Timeout (in milliseconds) for connection establishment
   * @param readTimeoutMs Socket read timeout (in milliseconds) of all requests. This low-level timeout defines how long a blocking read on the socket should wait for data.
   * NOTE: Google's API doesn't seem to always respect this limit, and it's not always clear which timeout applies 
   * (The query level timeout here or the global one as provided in the {@link BigQueryConnector}'s constructor).
   * @param defaultExecutionConfig The default query execution configuration, in case none is specified when calling the execution methods.
   * @param observer on the BQ query execution, can be null
   * @param ex The executor to run BQ requests on. Can be multi-threaded or direct executor, depending on the required threading policy.
   * It's the responsibility of the caller to shut down this executor.
   * @throws IOException In case that the connector can't be initialized
   */
  public BigQueryConnector(String projectId, long connTimeoutMs, long readTimeoutMs, QueryExecutionConfig defaultExecutionConfig, Consumer<BQQueryExecutionSummary> observer,
      ExecutorService ex) throws IOException {
    this.projectId = projectId;
    this.bigquery = buildService(connTimeoutMs, readTimeoutMs);
    this.defaultExecConfig = defaultExecutionConfig;
    this.observer = observer;
    this.providedExecutor = ex;
    this.ex = MoreExecutors.listeningDecorator(providedExecutor);
  }

  private BigQuery buildService(long connTimeoutMs, long readTimeoutMs) throws IOException {
    Credentials credentials = GoogleCredentials.getApplicationDefault();
    return BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(credentials)
        .setTransportOptions(HttpTransportOptions.newBuilder()
            .setConnectTimeout((int) connTimeoutMs)
            .setReadTimeout((int) readTimeoutMs).build())
        .build().getService();
  }

  /**
   * @return The project that this instance is bound to. All actions will be performed in the scope of this project.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * @return the default query execution configuration.
   * Use toBuilder().setXXX().setYYY().build() to create a copy with
   * a few settings changed.
   */
  public QueryExecutionConfig getDefaultQueryExecutionConfig() {
    return defaultExecConfig;
  }

  /**
   * @return The executor provided in the constructor (and owner by the caller)
   */
  public ExecutorService getExecutorService() {
    return providedExecutor;
  }
  
  @SuppressWarnings("unchecked")
  private <R, F> BQQueryResultFuture<R, F> executeAsync(BQQuery<R, F> query, QueryExecutionConfig config, boolean streamResults) {
    BQQueryResultFuture<R, F> bqFuture;
    try {
      ListenableFuture<BQResultsIterator<R, F>> listenableFuture = ex.submit(new QueryTask<>(query, config, streamResults));
      bqFuture = new BQQueryResultFuture<R, F>(listenableFuture);
    } catch (RejectedExecutionException e) { // Happens when the executor's queue can't accept additional jobs
      Class<? extends BQQuery<R, F>> cls = (Class<? extends BQQuery<R, F>>) query.getClass();
      ActionStatsCollector<QueryTerminationType> actionStats = getStatsFor(cls);
      actionStats.startAndEnd(QueryTerminationType.FAILED_CLIENT_RESOURCES_LIMIT);
      bqFuture = new BQQueryResultFuture<>(Futures.immediateFailedFuture(new ClientResourcesBQException()));
    }
    return bqFuture;
  }

  /**
   * Runs a query asynchronously, returning a future, which is both checked and listenable.
   * Note that once the future terminates successfully and provides its value, it's still not final, 
   * in the sense that resultset iteration may still produce errors, since server page requests are used
   * during iteration.
   * The caller may set a destination table reference and table expiration time in the
   * supplied query config object.
   *
   * @param query The query to execute
   * @param config The execution configuration
   * @return The future providing the query result or the query execution exception.
   * This future is both checked and listenable (see {@link CheckedFuture} and {@link ListenableFuture}).
   * Upon success, an iterator on result rows is provided by the future. Note that the iterator's next() method may throw a {@link QueryResultBrokenException}
   * in case that the connection with BQ is broken during result set streaming.
   * In Case of a DML(Data Manipulation Language) query, the future returns an empty iterator.
   */
  public <R, F> BQQueryResultFuture<R, F> executeAsync(BQQuery<R, F> query, QueryExecutionConfig config) {
    return executeAsync(query, config, true);
  }

  /**
   * Runs a query synchronously.
   * Note that once the call returns successfully and provides its value, it's still not final, 
   * in the sense that resultset iteration may still produce errors, since server page requests are used
   * during iteration.
   * The caller may set a destination table reference and table expiration time in the
   * supplied query config object.
   *
   * @param query The query to execute
   * @param config The execution configuration
   * @return The results iterator
   * @throws BQException
   * @throws InterruptedException 
   */
  public <R, F> BQResultsIterator<R, F> execute(BQQuery<R, F> query, QueryExecutionConfig config) throws InterruptedException, BQException {
    return executeAsync(query, config).checkedGet();
  }

  /**
   * Runs a query asynchronously without streaming results back.
   * Recommended for use only for DML queries or for queries which dump results to a table anyway.
   *
   * @param query The query to execute
   * @param config The execution configuration
   * @return The future to use for determining completion and completion type (successful/failed).
   */
  public <R, F> BQQueryResultFuture<R, F> executeNoStreamingAsync(BQQuery<R, F> query, QueryExecutionConfig config) {
    return executeAsync(query, config, false);
  }

  /**
   * Runs a query synchronously without streaming results back.
   * Recommended for use only for DML queries or for queries which dump results to a table anyway.
   *
   * @param query The query to execute
   * @param config The execution configuration
   * @return the record count
   * @throws BQException 
   * @throws InterruptedException 
   */
  public long executeNoStreaming(BQQuery<?, ?> query, QueryExecutionConfig config) throws InterruptedException, BQException {
    BQResultsIterator<?, ?> iterator = executeNoStreamingAsync(query, config).checkedGet();
    return iterator.totalRecordCount();
  }

  /**
   * Runs a query asynchronously, returning a future, which is both checked and listenable.
   * Note that once the future terminates successfully and provides its value, it's still not final, 
   * in the sense that resultset iteration may still produce errors, since server page requests are used
   * during iteration.
   * 
   * Uses the default query execution config as defined in the constructor
   *
   * @param query The query to execute
   * @return The future providing the query result or the query execution exception.
   * Upon success, an iterator on result rows is provided by the future. Note that the iterator's next() method may throw a {@link QueryResultBrokenException}
   * in case that the connection with BQ is broken during result set streaming.
   * In Case of a DML(Data Manipulation Language) query, the future returns an empty iterator.
   */
  public <R, F> BQQueryResultFuture<R, F> executeAsync(BQQuery<R, F> query) {
    return executeAsync(query, defaultExecConfig, true);
  }

  /**
   * Runs a query synchronously.
   * Note that once the call returns successfully and provides its value, it's still not final, 
   * in the sense that resultset iteration may still produce errors, since server page requests are used
   * during iteration.
   * 
   * Uses the default query execution config as defined in the constructor
   *
   * @param query The query to execute
   * @return The results iterator
   * @throws BQException 
   * @throws InterruptedException 
   */
  public <R, F> BQResultsIterator<R, F> execute(BQQuery<R, F> query) throws InterruptedException, BQException {
    return executeAsync(query).checkedGet();
  }

  /**
   * Runs a query asynchronously without streaming results back.
   * Recommended for use only for DML queries or for queries which dump results to a table anyway.
   * @param query The query to execute
   * @return The future to use for determining completion and completion type (successful/failed).
   */
  public CheckedFuture<Void, BQException> executeNoStreamingAsync(BQQuery<?, ?> query) {
    return new CheckedFutureTransformer<>(executeAsync(query, defaultExecConfig, false), s -> null);
  }

  /**
   * Runs a query synchronously without streaming results back.
   * Recommended for use only for DML queries or for queries which dump results to a table anyway.
   * @param query The query to execute
   * @throws BQException
   * @throws InterruptedException 
   */
  public void executeNoStreaming(BQQuery<?, ?> query) throws BQException, InterruptedException {
    executeNoStreamingAsync(query).checkedGet();
  }

  private static String labelizeName(String str0) {
    // remove characters that google does not allow (includes upper case characters)
    String str = str0.toLowerCase().replaceAll("[^a-z0-9_]", "");

    // If we lost characters from the original name, we want to add a CRC suffix
    // to prevent ambiguity. Note that this we also do this if the string is too long,
    // since google limits label length.
    if (str.length() > 63 || !str.equalsIgnoreCase(str0)) {

      // If we decided to add CRC, we must truncate string so that there is room for it.
      if (str.length() > 58) {
        str = str.substring(0, 58);
      }
      CRC32 crc = new CRC32();
      crc.update(str0.getBytes());

      // We only take 2 CRC bytes, it is enough.
      str = String.format("%s-%04x", str, crc.getValue() & 0xFFFFL);
    }
    return str;
  }

  private static void setJobConfigurationLabel(Builder jobConfiguration, String label) {
    Map<String, String> map = new HashMap<String, String>();
    map.put("name", label);
    map.put("lang", "java");

    jobConfiguration.setLabels(map);
  }

  /**
   * @param dataset A dataset name
   * @param table A table name
   * @return true iff the table exists
   */
  public boolean tableExists(String dataset, String table) {
    return bigquery.getTable(TableId.of(projectId, dataset, table)) != null;
  }

  /**
   * Sets table's expiration time. Table must exist.
   * @param datasetId the table's dataset id
   * @param tableName the table's name
   * @param duration number of units for table's deletion, measured from now. Must be greater than 0. Null means infinite.
   * @param timeUnit the time unit of the given duration.
   */
  public void updateTableExpiration(String datasetId, String tableName, Integer duration, TimeUnit timeUnit) {
    Table table = bigquery.getTable(TableId.of(projectId, datasetId, tableName));
    Long expirationTime = duration == null ? null : Clock.SYSTEM.currentTimeMillis() + timeUnit.toMillis(duration);    
    TableInfo definition = table.toBuilder().setExpirationTime(expirationTime).build();
    bigquery.update(definition);
  }

  @Override
  public JSONObject getOwnMetrics() {
    return new JSONObject();
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    Map<String, JsonMonitorable> queriesMap = new HashMap<>();
    for (Entry<Class<? extends BQQuery<?, ?>>, ActionStatsCollector<QueryTerminationType>> entry : stats.entrySet()) {
      String queryName = entry.getKey().getSimpleName();
      queriesMap.put(queryName, entry.getValue());
    }

    Map<String, JsonMonitorable> map = new HashMap<>();
    map.put("queries", new JsonMonitorableWrapper(queriesMap));
    map.put("exports", exportStats);
    map.put("loads", loadStats);
    return map;
  }

  private static void logQueryEnd(long durationMicro, Throwable e, String jobId, String qType, Long bytesProcessed, Long totalRows) {
    long durationSec = durationMicro / 1000_000;
    String completion = (e == null) ? "completed" : "failed";
    String job = (jobId == null) ? "N/A" : jobId;
    String bytes = (bytesProcessed == null) ? "N/A" : bytesProcessed.toString();
    String rows = (totalRows == null) ? "N/A" : totalRows.toString();
    log.debug("Query '" + qType + "' (Job ID " + job + ") execution " + completion + ". Duration: " + durationSec
        + " sec, Processed: " + bytes + " bytes, Rows: " + rows, e);
  }

  /**
   * @param c A big query request type
   * @return The stats collector for the given request type. Created and stored in the stats map if doesn't exist (atomically).
   */
  private ActionStatsCollector<QueryTerminationType> getStatsFor(Class<? extends BQQuery<?, ?>> c) {
    return stats.computeIfAbsent(c, 
        key -> new NonBlockingActionStatsCollector<>(QueryTerminationType.class));
  }

  /**
   * Runs an export job, asynchronously
   *
   * @param config The export configuration
   * @return The export future, which is checked and listenable
   */
  public BQExportFuture exportTableAsync(TableExportConfig config) {
    BQExportFuture bqFuture;
    try {
      ListenableFuture<Void> listenableFuture = ex.submit(new ExportTask(config));
      bqFuture = new BQExportFuture(listenableFuture);
    } catch (RejectedExecutionException e) { // Happens when the executor's queue can't accept additional jobs
      exportStats.startAndEnd(ExportTerminationType.FAILED_CLIENT_RESOURCES_LIMIT);
      bqFuture = new BQExportFuture(Futures.immediateFailedFuture(new ClientResourcesBQException()));
    }
    return bqFuture;
  }

  /**
   * Runs an export job, synchronously
   *
   * @param config The export configuration
   * @throws BQException 
   * @throws InterruptedException 
   */
  public void exportTable(TableExportConfig config) throws InterruptedException, BQException {
    exportTableAsync(config).checkedGet();
  }

  /**
   * Runs an async table load job
   *
   * @param tableLoadConfig the load job configuration. Serves for local or remote load from cloud storage.
   * @return The future for this async operation. This future is both checked
   * and listenable.
   */
  public BQTableLoadFuture loadTableAsync(TableLoadConfig tableLoadConfig) {
    BQTableLoadFuture bqFuture;
    try {
      ListenableFuture<Void> listenableFuture = ex.submit(new TableLoadTask(tableLoadConfig));
      bqFuture = new BQTableLoadFuture(listenableFuture);
    } catch (RejectedExecutionException e) { // Happens when the executor's queue can't accept additional jobs
      loadStats.startAndEnd(LoadTerminationType.FAILED_CLIENT_RESOURCES_LIMIT);
      bqFuture = new BQTableLoadFuture(Futures.immediateFailedFuture(new ClientResourcesBQException()));
    }
    return bqFuture;    
  }

  /**
   * Runs a synchronous table load
   *
   * @param tableLoadConfig the load job configuration. Serves for local or remote load from cloud storage.
   * @throws BQException 
   * @throws InterruptedException 
   */
  public void loadTable(TableLoadConfig tableLoadConfig) throws InterruptedException, BQException {
    loadTableAsync(tableLoadConfig).checkedGet();
  }

  // An executor submitable task for handling query requests
  // * Returns an empty result set iterator for DML queries (just as data queries with no results)
  // * Performs monitoring
  // * Throws only BQException, InterruptedException or runtime exceptions (unexpected - indicating a bug)
  private class QueryTask <R, F> implements Callable<BQResultsIterator<R, F>> {
    private final BQQuery<R, F> query;
    private final QueryExecutionConfig config;
    private final boolean streamResults;

    public QueryTask(BQQuery<R, F> query, QueryExecutionConfig config, boolean streamResults) {
      this.query = query;
      this.config = config;
      this.streamResults = streamResults;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public BQResultsIterator<R, F> call() throws BQException, InterruptedException {
      Class<? extends BQQuery<R, F>> qClass = (Class<? extends BQQuery<R, F>>) query.getClass();
      String qClassName = qClass.getSimpleName();
      ActionStatsCollector<QueryTerminationType> qStats = getStatsFor(qClass);
      qStats.start();
      long startTime = System.currentTimeMillis();
      Boolean isCacheHit = null;
      Long totalRows = null;
      Job job = null;
      Long bytes = null;
      String sql = null;
      BQException exc = null;
      boolean isBQSuccess = false;
      QueryTerminationType terminationType = QueryTerminationType.SUCCESS;
      try {
        sql = query.getSQL();
        log.debug("Executing '" + qClassName + "': " + sql);
        TableId tableRef = config.getDestinationTableReference();

        QueryJobConfiguration queryJobConfig = buildQueryJobConfig(sql, tableRef, config);
        job = bigquery.create(JobInfo.of(queryJobConfig));
     
        TableResult res = job.getQueryResults(QueryResultsOption.pageSize(config.getMaxRowsPerPage()));

        if (tableRef != null && config.getDestinationTableExpirationHs() != null) {
          updateTableExpiration(tableRef.getDataset(), tableRef.getTable(), config.getDestinationTableExpirationHs(), TimeUnit.HOURS);
        }
        
        // Request execution stats
        job = bigquery.getJob(job.getJobId(), JobOption.fields(JobField.STATISTICS));
        QueryStatistics qstats = ((QueryStatistics)job.getStatistics());
        
        bytes = qstats.getTotalBytesProcessed(); // 0 for cached results
        isCacheHit = qstats.getCacheHit();
        totalRows = res.getTotalRows();
        
        isBQSuccess = true;

        if (config.getMaxResults() != null && totalRows > config.getMaxResults()) { // Max results = null means no limit
          throw new ClientTooManyRowsBQException("Max rows limit (soft limit) exceeded. Maximum is "
              + config.getMaxResults() + ", but the result has " + totalRows + " rows.", query);
        }

        Iterator<FieldValueList> it = streamResults ? res.iterateAll().iterator() : Collections.emptyIterator();
        return new BQResultsIterator<>(query, Iterators.transform(it, query::mapRow), totalRows);
      } catch (Throwable e) { // Map exception and determine termination type
        Pair<BQException, QueryTerminationType> errDetails = mapQueryException(e, query);
        exc = errDetails.getLeft();
        terminationType = errDetails.getRight();
        if (terminationType == QueryTerminationType.INTERNAL_ERROR) {
          throw new RuntimeException(exc);
        }
        throw exc;
      } finally { // Register action (monitoring, logging, observer)
        String jobIdStr = job == null ? "N/A" : job.getJobId().getJob();
        long durationMicro = qStats.end(terminationType);
        logQueryEnd(durationMicro, exc, jobIdStr, qClassName, bytes, totalRows);
        if (observer != null) {
          observer.accept(new BQQueryExecutionSummary(startTime, jobIdStr, bytes, isCacheHit, totalRows, qClassName, sql, (float) durationMicro / 1000000, isBQSuccess));
        }
      }
    }
      
    // Analyzes a query originated exception in its original form (as produced by the QueryTask class), 
    // maps to a proper BQException descendant type, and finds the proper query termination type (for monitoring purposes)  
    // Input exceptions are handled as follows:
    // 1) InterruptedException - marked as TransientBQException, and status=FAILED_OTHER
    // 2) ClientTooManyRowsBQException - Same exception, and status=FAILED_CLIENT_TOO_MANY_ROWS
    // 3) JobException/BigQueryExcetion (google exceptions)
    // Mapped according to the specific type.
    // For details on the different server side errors, see https://cloud.google.com/bigquery/docs/error-messages
    // 4) A null value of the exception indicates no error, and is mapped to null exception and status=SUCCESS
    // 5) Unchecked exceptions are mapped to NonTransientBQException and status=INTERNAL_ERROR
    private Pair<BQException, QueryTerminationType> mapQueryException(Throwable e, BQQuery<?, ?> query) {
      if (e == null) {
        return new ImmutablePair<>(null, QueryTerminationType.SUCCESS);
      }
      
      if (e instanceof ClientTooManyRowsBQException) {
        return new ImmutablePair<>((BQException)e, QueryTerminationType.FAILED_CLIENT_TOO_MANY_ROWS);
      }
      
      if (e.getCause() instanceof SocketTimeoutException) {
        return new ImmutablePair<>(new TimeoutBQException("Query timed out.", query), QueryTerminationType.FAILED_TIMEOUT);
      }

      String reason = null;
      if (e instanceof JobException) {
        reason = ((JobException)e).getErrors().get(0).getReason();
      } else if (e instanceof BigQueryException) {
        reason = ((BigQueryException)e).getReason();
      } 

      if (reason != null) {
        switch (reason) {
          case "responseTooLarge":
            return new ImmutablePair<>(new ServerTooManyRowsBQException(e, query), QueryTerminationType.FAILED_SERVER_TOO_MANY_ROWS);
          case "notFound":
          case "invalid":
          case "invalidQuery":
            return new ImmutablePair<>(new InvalidQueryBQException("Illegal query: " + e.getMessage(), query), QueryTerminationType.FAILED_INVALID_QUERY);
          case "resourcesExceeded":
            return new ImmutablePair<>(new ServerResourcesBQException(e, query), QueryTerminationType.FAILED_SERVER_RESOURCES_LIMIT);
          case "backendError":
          case "internalError":
            return new ImmutablePair<>(new UnavailableBQException("BQ server side error", e), QueryTerminationType.FAILED_SERVER_ERROR);
          case "stopped" :
            return new ImmutablePair<>(new TimeoutBQException("Query timed out. Job stopped.", query), QueryTerminationType.FAILED_TIMEOUT);
        }
      }
      
      if (e instanceof RuntimeException || e instanceof Error) {
        return new ImmutablePair<>(new NonTransientBQException("Internal error", e), QueryTerminationType.INTERNAL_ERROR);
      }

      return new ImmutablePair<>(new TransientBQException("Unspecified error", e), QueryTerminationType.FAILED_OTHER);
    }
    
    private QueryJobConfiguration buildQueryJobConfig(String sql, TableId destinationTableRef, QueryExecutionConfig config) {
      Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(sql)
          .setUseQueryCache(!config.isCacheDisabled())
          .setUseLegacySql(query.isLegacySQL())
          .setPriority(config.getPriority())
          .setJobTimeoutMs(config.getTimeoutMs()); // TODO: Doesn't seem to really work. Google's documentation doesn't help much either.
                                                   // However, global transport config does have an effect

      Map<String, QueryParameterValue> queryParameters = query.getQueryParameters();
      if (queryParameters != null) {
        queryConfigBuilder
            .setNamedParameters(queryParameters);
      }

      if (destinationTableRef != null) {
        queryConfigBuilder.setDestinationTable(destinationTableRef).setAllowLargeResults(true).setWriteDisposition(config.getWriteDisposition());
        if (log.isDebugEnabled()) {
          log.debug("query results are saved to: " + destinationTableRef.getDataset() + "." + destinationTableRef.getTable() + " table.");
        }
      }

      setJobConfigurationLabel(queryConfigBuilder, labelizeName(query.getClass().getSimpleName()));
      return queryConfigBuilder.build();
    }
  }
  
  // A checked future supplying the iterator over BQ query results
  public static class BQQueryResultFuture<R, F> extends AbstractCheckedFuture<BQResultsIterator<R, F>, BQException> {
    public BQQueryResultFuture(ListenableFuture<BQResultsIterator<R, F>> future) {
      super(future);
    }

    // No mapping to be done since it's already handled previously
    // and all exceptions here are either of type RuntimeException or BQException
    @Override
    protected BQException map(Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      }
      return (BQException) e;
    }
  }

  // An executor submitable task for handling export requests
  // * Performs monitoring
  // * Throws only BQException, InterruptedException or runtime exceptions (unexpected - indicating a bug)
  private class ExportTask implements Callable<Void> {
    private final TableExportConfig exportConfig;

    public ExportTask(TableExportConfig exportConfig) {
      this.exportConfig = exportConfig;
    }
    
    @Override
    public Void call() throws BQException, InterruptedException {
      ExportTerminationType terminationType = ExportTerminationType.SUCCESS;
      BQException exc = null;
      Job job = null;
      log.debug("Executing export: " + exportConfig);
      exportStats.start();
      try {
        ExtractJobConfiguration config = ExtractJobConfiguration.newBuilder(
            exportConfig.getSourceTableReference(), new ArrayList<>(exportConfig.getDestinationURIs()))
        .setCompression(exportConfig.getCompression().name())
        .setFieldDelimiter(String.valueOf(exportConfig.getCSVFieldDelimiter()))
        .setFormat(exportConfig.getExportFormat().name())
        .setJobTimeoutMs(exportConfig.getTimeoutMs())
        .setPrintHeader(exportConfig.isPrintHeader()).build();
        
        job = bigquery.create(JobInfo.of(config));
        job = job.waitFor();
        validateJobStatus(job);
        return null;
      } catch (Throwable e) {
        Pair<BQException, ExportTerminationType> errDetails = mapExportException(e, exportConfig);
        exc = errDetails.getLeft();
        terminationType = errDetails.getRight();
        if (terminationType == ExportTerminationType.INTERNAL_ERROR) {
          throw new RuntimeException(exc);
        }
        throw exc;
      } finally { // Register action (monitoring, logging, observer)
        String jobIdStr = job == null ? "N/A" : job.getJobId().getJob();
        long durationMicro = exportStats.end(terminationType);
        log.debug("Done export (job=" + jobIdStr + ") " + (exc == null ? "without" : "with") + " error. Duration: " + (durationMicro / 1000) + " ms");
      }
    }
    
    // Analyzes an export originated exception in its original form (as produced by the ExportTask class), 
    // maps to a proper BQException descendant type, and finds the proper termination type (for monitoring purposes)  
    // Input exceptions are handled as follows:
    // 1) InterruptedException - marked as TransientBQException, and status=FAILED_OTHER
    // 2) JobException/BigQueryExcetion (google exceptions)
    // Mapped according to the specific type.
    // For details on the different server side errors, see https://cloud.google.com/bigquery/docs/error-messages
    // 3) A null value of the exception indicates no error, and is mapped to null exception and status=SUCCESS
    // 4) Unchecked exceptions are mapped to NonTransientBQException and status=INTERNAL_ERROR
    private Pair<BQException, ExportTerminationType> mapExportException(Throwable e, TableExportConfig config) {
      if (e == null) {
        return new ImmutablePair<>(null, ExportTerminationType.SUCCESS);
      }
      
      if (e.getCause() instanceof SocketTimeoutException) {
        return new ImmutablePair<>(new TimeoutBQException("Export timed out.", e.getCause()), ExportTerminationType.FAILED_TIMEOUT);
      }

      String reason = null;
      if (e instanceof JobException) {
        reason = ((JobException)e).getErrors().get(0).getReason();
      } else if (e instanceof BigQueryException) {
        reason = ((BigQueryException)e).getReason();
      } 

      if (reason != null) {
        switch (reason) {
          case "notFound":
          case "invalid":
            return new ImmutablePair<>(new InvalidExportBQException("Illegal export request: " + e.getMessage(), config), ExportTerminationType.FAILED_INVALID_REQUEST);
          case "backendError":
          case "internalError":
            return new ImmutablePair<>(new UnavailableBQException("BQ server side error", e), ExportTerminationType.FAILED_SERVER_ERROR);
          case "stopped" :
            return new ImmutablePair<>(new TimeoutBQException("Export timed out. Job stopped.", e.getCause()), ExportTerminationType.FAILED_TIMEOUT);
        }
      }
      
      if (e instanceof RuntimeException || e instanceof Error) {
        return new ImmutablePair<>(new NonTransientBQException("Internal error", e), ExportTerminationType.INTERNAL_ERROR);
      }

      return new ImmutablePair<>(new TransientBQException("Unspecified error", e), ExportTerminationType.FAILED_OTHER);
    }
  }

  // A checked future for export requests
  public static class BQExportFuture extends AbstractCheckedFuture<Void, BQException> {
    public BQExportFuture(ListenableFuture<Void> future) {
      super(future);
    }

    // No mapping to be done since it's already handled previously
    // and all exceptions here are either of type RuntimeException or BQException
    @Override
    protected BQException map(Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      }
      return (BQException) e;
    }
  }

  // An executor submitable task for handling table load requests
  // * Performs monitoring
  // * Throws only BQException, InterruptedException or runtime exceptions (unexpected - indicating a bug)
  private class TableLoadTask implements Callable<Void> {
    private final TableLoadConfig loadConfig;

    public TableLoadTask(TableLoadConfig loadConfig) {
      this.loadConfig = loadConfig;
    }
    
    @Override
    public Void call() throws BQException, InterruptedException {
      LoadTerminationType terminationType = LoadTerminationType.SUCCESS;
      BQException exc = null;
      Job job = null;
      log.debug("Executing table load: " + loadConfig);
      loadStats.start();
      try {
        TableId destinationTable = loadConfig.getDestinationTableReference();
        if (loadConfig.isRemoteLoad()) {
          LoadJobConfiguration.Builder bqConfig = LoadJobConfiguration.newBuilder(
              destinationTable, new ArrayList<>(loadConfig.getSourceURIs()))
              .setSchemaUpdateOptions(Collections.singletonList(SchemaUpdateOption.ALLOW_FIELD_ADDITION)) // Allow the provided schema to add new optional fields
              .setCreateDisposition(loadConfig.getCreateDisposition())
              .setWriteDisposition(loadConfig.getWriteDisposition())
              .setDestinationTable(loadConfig.getDestinationTableReference())
              .setFormatOptions(getFormatOptions(loadConfig))
              .setJobTimeoutMs(loadConfig.getTimeoutMs())
              .setSchema(loadConfig.getTableSchema())
              .setSourceUris(new ArrayList<>(loadConfig.getSourceURIs()));
          
          setPartitionConfig(bqConfig, loadConfig);
          setClusteringConfig(bqConfig, loadConfig);

          job = bigquery.create(JobInfo.of(bqConfig.build()));
        } else { // Local load
          WriteChannelConfiguration.Builder bqLocalConfig = WriteChannelConfiguration.newBuilder(
          destinationTable)
          .setSchemaUpdateOptions(Collections.singletonList(SchemaUpdateOption.ALLOW_FIELD_ADDITION)) // Allow the provided schema to add new optional fields
          .setCreateDisposition(loadConfig.getCreateDisposition())
          .setWriteDisposition(loadConfig.getWriteDisposition())
          .setDestinationTable(loadConfig.getDestinationTableReference())
          .setFormatOptions(getFormatOptions(loadConfig))
          .setSchema(loadConfig.getTableSchema());
          
          setPartitionConfig(bqLocalConfig, loadConfig);
          setClusteringConfig(bqLocalConfig, loadConfig);
          
          WriteChannelConfiguration writeConfig = bqLocalConfig.build();
          JobId jobId = JobId.newBuilder().build();
          TableDataWriteChannel writer = bigquery.writer(jobId, writeConfig);
          try {
            writeFilesToWriter(writer);
          } finally {
            writer.close();
          }
          job = writer.getJob();
        }
        job = job.waitFor();
        validateJobStatus(job);
        
        // Update table's expiration
        updateTableExpiration(destinationTable.getDataset(), destinationTable.getTable(), 
            loadConfig.getDestinationTableExpirationHs(), TimeUnit.HOURS);
        
        return null;
      } catch (Throwable e) {
        Pair<BQException, LoadTerminationType> errDetails = mapLoadException(e, loadConfig);
        exc = errDetails.getLeft();
        terminationType = errDetails.getRight();
        if (terminationType == LoadTerminationType.INTERNAL_ERROR) {
          throw new RuntimeException(exc);
        }
        throw exc;
      } finally { // Register action (monitoring, logging, observer)
        String jobIdStr = job == null ? "N/A" : job.getJobId().getJob();
        long durationMicro = loadStats.end(terminationType);
        log.debug("Done table load (job=" + jobIdStr + ") " + (exc == null ? "without" : "with") + " error. Duration: " + (durationMicro / 1000) + " ms");
      }
    }

    private void setPartitionConfig(LoadConfiguration.Builder bqConfig, TableLoadConfig loadConfig) {
      LocalDate date = loadConfig.getDestinationTablePartition();
      if (date != null) {
        TableId tableId = loadConfig.getDestinationTableReference();
        tableId = TableId.of(tableId.getDataset(), tableId.getTable() + "$" + PARTITION_DATE_FORMAT.format(date));
        bqConfig.setDestinationTable(tableId);
        bqConfig.setTimePartitioning(TimePartitioning.of(Type.DAY));
      }
    }

    private void setClusteringConfig(LoadConfiguration.Builder bqConfig, TableLoadConfig loadConfig) {
      Set<String> fields = loadConfig.getClusteringFields();
      if (fields != null && !fields.isEmpty()) {
        bqConfig.setClustering(Clustering.newBuilder().setFields(new ArrayList<>(fields)).build());
      }
    }

    private FormatOptions getFormatOptions(TableLoadConfig loadConfig) {
      LoadFormat format = loadConfig.getLoadFormat();
      if (format == LoadFormat.CSV) {
        return CsvOptions.newBuilder().setFieldDelimiter(loadConfig.getCsvFieldDelimiter()).setAllowJaggedRows(loadConfig.getAllowJaggedRows()).setSkipLeadingRows(loadConfig.getCSVHasHeader() ? 1 : 0).build();
      } else {
        return FormatOptions.of(loadConfig.getLoadFormat().name());
      }
    }

    // Throws FileNotFoundException in case of no matching file to load
    private void writeFilesToWriter(TableDataWriteChannel writer) throws IOException {
      File[] files = null;
      try (OutputStream os = Channels.newOutputStream(writer)) {
        for (String localPath : loadConfig.getSourceURIs()) {
          String filenamePart = PathUtils.getLastPathPart(localPath);
          String parentPath = PathUtils.getParentPath(localPath);
          FileFilter fileFilter = new WildcardFileFilter(filenamePart);
          files = new File(parentPath).listFiles(fileFilter);
          for (File f : files) {
            try (InputStream is = FileUtils.getInputStream(f, new FileReadOptions().detectCompression(filenamePart))) {
              IOUtils.copy(is, os);
            }
          }
          if (files == null || files.length == 0) {
            throw new FileNotFoundException();
          }
        }
      }
    }
    
    // Analyzes a table load originated exception in its original form (as produced by the LoadTask class), 
    // maps to a proper BQException descendant type, and finds the proper termination type (for monitoring purposes)  
    // Input exceptions are handled as follows:
    // 1) InterruptedException - marked as TransientBQException, and status=FAILED_OTHER
    // 2) JobException/BigQueryExcetion (google exceptions)
    // Mapped according to the specific type.
    // For details on the different server side errors, see https://cloud.google.com/bigquery/docs/error-messages
    // 3) FileNotFoundException - Mapped to InvalidTableLoadBQException (refers to local upload with no files).
    // 4) IOException - Mapped to IOTransientBQException. We can't be sure it's a transient situation, 
    // but good chances that it is, and we can't tell for sure in this case.
    // 5) A null value of the exception indicates no error, and is mapped to null exception and status=SUCCESS
    // 6) Unchecked exceptions are mapped to NonTransientBQException and status=INTERNAL_ERROR
    private Pair<BQException, LoadTerminationType> mapLoadException(Throwable e, TableLoadConfig config) {
      if (e == null) {
        return new ImmutablePair<>(null, LoadTerminationType.SUCCESS);
      }
      
      if (e.getCause() instanceof SocketTimeoutException) {
        return new ImmutablePair<>(new TimeoutBQException("Table load timed out.", e.getCause()), LoadTerminationType.FAILED_TIMEOUT);
      }

      String reason = null;
      if (e instanceof JobException) {
        reason = ((JobException)e).getErrors().get(0).getReason();
      } else if (e instanceof BigQueryException) {
        reason = ((BigQueryException)e).getReason();
      } 

      if (reason != null) {
        switch (reason) {
          case "notFound":
          case "invalid":
            return new ImmutablePair<>(new InvalidTableLoadBQException("Illegal table load request: " + e.getMessage(), config), LoadTerminationType.FAILED_INVALID_REQUEST);
          case "backendError":
          case "internalError":
            return new ImmutablePair<>(new UnavailableBQException("BQ server side error", e), LoadTerminationType.FAILED_SERVER_ERROR);
          case "stopped" :
            return new ImmutablePair<>(new TimeoutBQException("Table load timed out. Job stopped.", e.getCause()), LoadTerminationType.FAILED_TIMEOUT);
        }
      }
      
      if (e instanceof FileNotFoundException) {
        return new ImmutablePair<>(new InvalidTableLoadBQException("Local load with no matching files", config), LoadTerminationType.FAILED_OTHER);
      }
      if (e instanceof IOException) {
        return new ImmutablePair<>(new IOTransientBQException("IO error", e), LoadTerminationType.FAILED_OTHER);
      }
      
      if (e instanceof RuntimeException || e instanceof Error) {
        return new ImmutablePair<>(new NonTransientBQException("Internal error", e), LoadTerminationType.INTERNAL_ERROR);
      }

      return new ImmutablePair<>(new TransientBQException("Unspecified error", e), LoadTerminationType.FAILED_OTHER);
    }
  }

  // A checked future for table load requests
  public static class BQTableLoadFuture extends AbstractCheckedFuture<Void, BQException> {
    public BQTableLoadFuture(ListenableFuture<Void> future) {
      super(future);
    }

    // No mapping to be done since it's already handled previously
    // and all exceptions here are either of type RuntimeException or BQException
    @Override
    protected BQException map(Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      }
      return (BQException) e;
    }
  }

  // Checks whether the job contains a "silent error". If so, throws it as a BigQueryException, including the concatenated error messages.
  // For some reason google's code hides some real errors, and doesn't throw an exception for them (in load/export for example).
  private void validateJobStatus(Job job) {
    BigQueryError err = job.getStatus().getError();
    if (err != null) {
      String concatenatedMsgs = job.getStatus().getExecutionErrors().stream().map(BigQueryError::getMessage).collect(Collectors.joining("\n"));
      throw new BigQueryException(400, concatenatedMsgs, err); // A generic "user error" code. Not used anyway. 
    }
  }  
}
