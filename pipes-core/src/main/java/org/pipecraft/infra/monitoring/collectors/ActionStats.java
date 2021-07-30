package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.Collections;
import java.util.Map;
import net.minidev.json.JSONObject;

/**
 * Simple action statistics bean referring to a specific action type.
 * 
 * Immutable.
 * 
 * @author Eyal Schneider
 */
public final class ActionStats implements JsonMonitorable {
  private final long completedCount;
  private final long totalDurationMicros;
  
  /**
   * Constructor
   * 
   * @param completedCount The number of completed actions
   * @param totalDurationMicros The total duration of completed actions, in microseconds
   */
  public ActionStats(long completedCount, long totalDurationMicros) {
    this.completedCount = completedCount;
    this.totalDurationMicros = totalDurationMicros;
  }

  /**
   * @return The number of completed actions
   */
  public long getCompletedCount() {
    return completedCount;
  }

  /**
   * @return The total duration of completed actions, in microseconds
   */
  public long getTotalDurationMicros() {
    return totalDurationMicros;
  }
  
  /**
   * @return The average time (in microseconds) of an action execution. -1 is returned if no action has been completed yet.
   */
  public long getAvgActionDuration() {
    if (completedCount == 0)
      return -1;
    return totalDurationMicros / completedCount;
  }
  
  @Override
  public String toString() {
    return getOwnMetrics().toJSONString();
  }

  @Override
  public JSONObject getOwnMetrics() {
    JSONObject res = new JSONObject();
    res.put("completed", completedCount);
    res.put("totalMicros", totalDurationMicros);
    res.put("avgMicros", completedCount > 0 ? totalDurationMicros / completedCount : 0);
    return res; 
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }
}
