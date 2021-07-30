package org.pipecraft.infra.monitoring.graphite;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple graphite client that sends metrics to graphite server.
 * @author Michal Rockban
 */
public class GraphiteClient {

  private static final String EMPTY_ROOT = "";
  private static final String METRIC_PATH_SEPARATOR = ".";
  private final String host;
  private final int port;
  private final String rootPath;

  /**
   * C-tor
   * @param host the graphite host machine
   * @param port the graphite port
   * @param rootPath the graphite root path environment (i.e qa/prod/quality, etc). Nullable
   */
  public GraphiteClient(String host, int port, String rootPath) {
    this.host = host;
    this.port = port;
    this.rootPath = rootPath;
  }


  /**
   * Sends the metric to graphite, under environment root path, in the current timestamp.
   * @param key metric path
   * @param value metric value
   */
  public void sendMetric(String key, Number value) throws GraphiteException {
    sendMetric(key, value, getCurrentTimestamp());
  }


  /**
   * Sends the metrics to graphite, under environment root path, in the current timestamp.
   * @param metrics metric path and key map
   */
  public void sendMetrics(Map<String, ? extends Number> metrics) throws GraphiteException {
    sendMetrics(metrics, getCurrentTimestamp());
  }

  /**
   * Sends the metric to graphite, under environment root path
   * @param metrics metric path and key map
   * @param timestamp the timestampe, in seconds since epoch.
   */
  public void sendMetrics(Map<String, ? extends Number> metrics, long timestamp) throws GraphiteException {
    try (Socket socket = new Socket(host, port);
         PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
      for (Entry<String, ? extends Number> metric : metrics.entrySet()) {
        out.printf("%s %s %d%n", getMetricName(metric), metric.getValue(), timestamp);
      }
    } catch (UnknownHostException e) {
      throw new GraphiteException("Unknown host: " + host);
    } catch (IOException e) {
      throw new GraphiteException("Error while writing data to graphite: " + e.getMessage(), e);
    }
  }

  private String getMetricName(Entry<String, ? extends Number> metric) {
    String prefix = rootPath == null ? EMPTY_ROOT : rootPath + METRIC_PATH_SEPARATOR;
    return prefix + metric.getKey();
  }


  /**
   * Sends the metric to graphite, under environment root path, in the given timestamp.
   * @param key metric path
   * @param value metric value
   * @param timestamp the timestamp (in seconds since epoch)
   */
  @SuppressWarnings("serial")
  public void sendMetric(final String key, final Number value, long timestamp) throws GraphiteException {
    sendMetrics(new HashMap<>() {{
      put(key, value);
    }}, timestamp);
  }

  protected long getCurrentTimestamp() {
    return System.currentTimeMillis() / 1000;
  }
}
