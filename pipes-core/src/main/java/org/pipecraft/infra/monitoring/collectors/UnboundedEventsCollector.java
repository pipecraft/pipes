package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import net.minidev.json.JSONObject;

/**
 * Collects counts on an unbounded set of categories. Unlike {@link EventsCollector}, which works with a closed set defined by 
 * an enum, here we allow values that aren't known in advance. This may results in bigger memory consumption, so this utility
 * should be used with caution.
 * 
 * This collector also supports an atomic reset operation for a given category
 * 
 * @param <T> The data type representing the different categories. Must be suitable for hash structure keys!
 * 
 * Thread safe.
 * 
 * @author Eyal Schneider
 */
public class UnboundedEventsCollector<T> implements JsonMonitorable {
  private final ConcurrentHashMap<T, AtomicLong> counters = new ConcurrentHashMap<T, AtomicLong>();

  /**
   * @return The set of categories currently maintained by this collector. Read only.
   */
  public Set<T> getCategories() {
    return Collections.unmodifiableSet(counters.keySet());
  }

  /**
   * Increments an event counter
   * @param category The event category
   * @param multiplicity The amount to increment by
   */
  public void countEvent(T category, long multiplicity) {
    AtomicLong counter = counters.get(category);
    if (counter == null) {
      counter = counters.computeIfAbsent(category, k -> new AtomicLong()); // Actually I could have used this method directly, but I'm still not sure it performs as well as get(..).
    }
    
    counter.addAndGet(multiplicity);
  }
  
  /**
   * Resets a counter.
   * @param category The counter category
   * @return The last value seen before the counter was reset. The retrieval of last value and the reseting are performed
   * as a single atomic operation.
   */
  public long reset(T category) {
    AtomicLong counter = counters.get(category);
    if (counter != null)
      return counter.getAndSet(0);
    
    return 0;
  }
  
  /**
   * @param category An event category
   * @return The current count in the given category
   */
  public long getCount(T category) {
    AtomicLong counter = counters.get(category);
    if (counter == null)
      return 0;
    return counter.get();
  }
  
  /**
   * @return A snapshot of the event counts map.
   * Not necessarily consistent.
   */
  public EventStatsMap<T> getAll() {    
    EventStatsMap<T> stats = new UnboundedEventStatsMap<T>();
    for (Map.Entry<T, AtomicLong> entry : counters.entrySet()) {
      T category = entry.getKey();
      AtomicLong counter = counters.get(category);
      stats.setCategoryCounts(entry.getKey(), counter.get());
    }
    return stats;
  }

  @Override
  public JSONObject getOwnMetrics() {
    return getAll().toJson();
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }
}
