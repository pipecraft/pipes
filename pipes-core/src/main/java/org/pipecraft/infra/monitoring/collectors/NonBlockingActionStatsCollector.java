package org.pipecraft.infra.monitoring.collectors;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A statistics collector which uses a non-blocking approach to improve performance. 
 * The getters don't guarantee data consistency, but they are accurate enough once enough data is gathered.
 * 
 * @author Eyal Schneider
 *
 * @param <E> The enum describing the different categories to be tracked
 */
public class NonBlockingActionStatsCollector<E extends Enum<E>> extends AbstractActionStatsCollector<E>{
  private final AtomicInteger running = new AtomicInteger();
  private final EnumMap<E, StatsPair> categoryStats;
  
  /**
   * Constructor
   * @param categoriesEnumClass the enum describing the different categories to be tracked
   */
  public NonBlockingActionStatsCollector(Class<E> categoriesEnumClass) {
    super(categoriesEnumClass);
    EnumMap<E, StatsPair> stats = new EnumMap<E, StatsPair>(categoriesEnumClass);
    for (E category : categoriesEnumClass.getEnumConstants())
      stats.put(category, new StatsPair());
    categoryStats = stats; //Java Memory model guarantees visibility of final fields provided they are assigned after being fully initialized
  }
  
  @Override
  protected void innerStart() {
    running.incrementAndGet();
  }

  @Override
  protected void innerEnd(E category, long duration) {
    running.decrementAndGet();
    StatsPair pair = categoryStats.get(category);
    pair.count.incrementAndGet();
    pair.totalDurationMicros.addAndGet(duration);
  }

  @Override
  public void startAndEnd(E category) {
    StatsPair pair = categoryStats.get(category);
    pair.count.incrementAndGet();
  }

  @Override
  public int getRunning() {
    return running.get();
  }

  @Override
  public long getCompletedCount(E category) {
    return categoryStats.get(category).count.get();
  }

  @Override
  public long getTotalDurationMicros(E category) {
    return categoryStats.get(category).totalDurationMicros.get();
  }

  @Override
  public ActionStatsMap<E> getAll() {    
    ActionStatsMap<E> stats = new ActionStatsMap<E>(categoriesEnumClass);
    stats.setRunningCount(getRunning());
    for (Map.Entry<E, StatsPair> entry : categoryStats.entrySet()) {
      E category = entry.getKey();
      StatsPair pair = categoryStats.get(category);
      stats.setCategoryStats(entry.getKey(), pair.count.get(), pair.totalDurationMicros.get());
    }
    return stats;
  }

  @Override
  public void clear() {
    running.set(0);
    for (StatsPair pair : categoryStats.values()) {
      pair.count.set(0);
      pair.totalDurationMicros.set(0);
    }
  }

  // Encapsulates the actions count and total duration
  private static class StatsPair {
    public final AtomicLong count = new AtomicLong();
    public final AtomicLong totalDurationMicros = new AtomicLong();
  }
}
