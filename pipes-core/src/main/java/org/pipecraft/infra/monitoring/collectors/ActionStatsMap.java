package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import org.pipecraft.infra.monitoring.JsonMonitorableWrapper;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import net.minidev.json.JSONObject;

/**
 * A collection of simple statistics gathered on multiple categories. Relevant for actions that have a duration and terminate with some outcome type,
 * belonging to a predefined set of possible types.
 * 
 * Not thread safe.
 * 
 * @author Eyal Schneider
 *
 * @param <E> The enum class representing all possible categories that stats are being gathered on
 */
public class ActionStatsMap<E extends Enum<E>> implements JsonMonitorable {
  private int runningCount;
  private int totalCompletedCount;
  private final EnumMap<E, ActionStats> statsPerCat;
  
  /**
   * Constructor
   * 
   * @param enumType The type of the enum describing all available categories
   */
  public ActionStatsMap(Class<E> enumType) {
    statsPerCat = new EnumMap<E, ActionStats>(enumType);
  }

  /**
   * @return The number of running actions
   */
  public int getRunningCount() {
    return runningCount;
  }

  /**
   * @return The total number of completed actions
   */
  public int getTotalCompletedCount() {
    return totalCompletedCount;
  }

  /**
   * @param category A statistics category
   * @return the category statistics  
   */
  public ActionStats getStats(E category) {
    return statsPerCat.get(category);
  }

  /**
   * @return the category statistics (read only) 
   */
  public Map<E, ActionStats> getCategoryStats() {
    return Collections.unmodifiableMap(statsPerCat);
  }

  /**
   * @param runningCount The number of running actions
   */
  public void setRunningCount(int runningCount) {
    this.runningCount = runningCount;
  }
  
  /**
   * Sets the statistics for a specific category.
   * @param category A stats category
   * @param completed The number of completed actions in the given category
   * @param totalDuration The total duration, in microseconds, of the completed actions of the given category
   */
  public void setCategoryStats(E category, long completed, long totalDuration) {
    ActionStats actionStats = new ActionStats(completed, totalDuration);
    ActionStats prev = statsPerCat.put(category, actionStats);
    if (prev != null)
      totalCompletedCount -= prev.getCompletedCount();
    totalCompletedCount += completed;
  }
  
  @Override
  public String toString() {
    return getFullMetrics().toJSONString();
  }

  @Override
  public JSONObject getOwnMetrics() {
    JSONObject res = new JSONObject();
    res.put("running", runningCount);
    res.put("totalCompleted", totalCompletedCount);
    return res;
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    Map<String, JsonMonitorable> map = new HashMap<>();
    for (Map.Entry<E, ActionStats> entry : statsPerCat.entrySet())
      map.put(entry.getKey().toString(), entry.getValue());
    
    JsonMonitorableWrapper wrapper = new JsonMonitorableWrapper(map);
    Map<String, JsonMonitorable> res = new HashMap<>();
    res.put("perCategory", wrapper);
    
    return res;
  }
}
