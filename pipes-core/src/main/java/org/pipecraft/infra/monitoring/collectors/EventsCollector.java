package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import net.minidev.json.JSONObject;

/**
 * Counts events that fall into one of a predefined set of categories.
 * Snapshots aren't guaranteed to be consistent.
 * 
 * @param E The enum describing the different event types 
 * 
 * @author Eyal Schneider
 *
 */
public class EventsCollector <E extends Enum<E>> implements JsonMonitorable {
  private final EnumMap<E, AtomicLong> categoryCounters;
  private final Class<E> categoriesEnumClass;
  
  /**
   * Constructor
   * @param categoriesEnumClass the enum describing the different categories to be tracked
   */
  public EventsCollector(Class<E> categoriesEnumClass) {
    this.categoriesEnumClass = categoriesEnumClass;
    EnumMap<E, AtomicLong> stats = new EnumMap<E, AtomicLong>(categoriesEnumClass);
    for (E category : categoriesEnumClass.getEnumConstants())
      stats.put(category, new AtomicLong());
    categoryCounters = stats; //Java Memory model guarantees visibility of final fields provided they are assigned after being fully initialized
  }

  /**
   * Indicates that an event of a given category just occurred.
   * @param category The category of the event
   */
  public void countEvent(E category) {
    AtomicLong counter = categoryCounters.get(category);
    counter.incrementAndGet();
  }

  /**
   * @param category A category
   * @return The number of events that have been counted in the given category
   */
  public long getCount(E category) {
    return categoryCounters.get(category).get();
  }

  /**
   * @return A snapshot of the event counts map.
   * Not necessarily consistent.
   */
  public EventStatsMap<E> getAll() {    
    EventStatsMap<E> stats = new EnumEventStatsMap<E>(categoriesEnumClass);
    for (Map.Entry<E, AtomicLong> entry : categoryCounters.entrySet()) {
      E category = entry.getKey();
      AtomicLong counter = categoryCounters.get(category);
      stats.setCategoryCounts(entry.getKey(), counter.get());
    }
    return stats;
  }

  /**
   * Clears all counters.
   * Not guaranteed to run atomically.
   */
  public void clear() {
    for (AtomicLong counter : categoryCounters.values())
      counter.set(0);
  }

  @Override
  public JSONObject getOwnMetrics() {
    return getAll().toJson();
  }

  @Override
  public Map<String, JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }
}
