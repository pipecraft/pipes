package org.pipecraft.infra.monitoring.collectors;

import java.util.Collections;
import java.util.Map;
import net.minidev.json.JSONObject;

/**
 * A mapping between event categories and their respective counts. 
 * 
 * Not thread safe.
 * 
 * @author Eyal Schneider
 *
 * @param <E> The class representing all possible event categories
 */
public abstract class EventStatsMap<E> {
  private final Map<E, Long> countsPerCat;
  
  /**
   * Constructor (protected)
   * 
   * @param map The map implementation to use internally
   */
  protected EventStatsMap(Map<E, Long> map) {
    this.countsPerCat = map;
  }

  /**
   * @param category A statistics category
   * @return the count in the given category  
   */
  public Long getCount(E category) {
    return countsPerCat.get(category);
  }

  /**
   * @return the category statistics (read only) 
   */
  public Map<E, Long> getCategoryStats() {
    return Collections.unmodifiableMap(countsPerCat);
  }

  /**
   * Sets the count for a specific category.
   * @param category A stats category
   * @param count The number of events in the given category
   */
  public void setCategoryCounts(E category, long count) {
    countsPerCat.put(category, count);
  }
  
  @Override
  public String toString() {
    return toJson().toJSONString();
  }

  /**
   * @return The contents of this object, as json
   */
  public JSONObject toJson() {
    JSONObject res = new JSONObject();
    for (Map.Entry<E, Long> entry : countsPerCat.entrySet())
      res.put(entry.getKey().toString(), entry.getValue());
    return res;
  }
  
  /**
   * Adds the counts from the other collector. Not atomic.
   * @param other The other collector to get the counters from
   */
  public void add(EventStatsMap<E> other) {
    for (Map.Entry<E, Long> entry : other.countsPerCat.entrySet()) {
      Long count = countsPerCat.get(entry.getKey());
      if (count == null) 
        count = (long)0;
      countsPerCat.put(entry.getKey(), count + entry.getValue());
    }
  }

}
