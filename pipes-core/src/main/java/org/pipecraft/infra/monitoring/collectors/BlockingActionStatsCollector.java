package org.pipecraft.infra.monitoring.collectors;

import java.util.EnumMap;
import java.util.Map;

/**
 * A statistics collector which uses synchronization locks for guaranteeing data consistency.
 * 
 * @author Eyal Schneider
 *
 * @param <E> The enum describing the different categories to be tracked
 */
public class BlockingActionStatsCollector<E extends Enum<E>> extends AbstractActionStatsCollector<E>{
  private int running;
  private final EnumMap<E, StatsPair> categoryStats;
  private final Object lock = new Object();
  
  /**
   * Constructor
   * @param categoriesEnumClass the enum describing the different categories to be tracked
   */
  public BlockingActionStatsCollector(Class<E> categoriesEnumClass) {
    super(categoriesEnumClass);
    EnumMap<E, StatsPair> stats = new EnumMap<E, StatsPair>(categoriesEnumClass);
    for (E category : categoriesEnumClass.getEnumConstants())
      stats.put(category, new StatsPair());
    categoryStats = stats; //Java Memory model guarantees visibility of final fields provided they are assigned after being fully initialized
  }
  
  @Override
  protected void innerStart() {
    synchronized(lock) {
      running++;
    }
  }

  @Override
  protected void innerEnd(E category, long duration) {
    synchronized(lock) {      
      running--;
      StatsPair pair = categoryStats.get(category);
      pair.count++;
      pair.totalDurationMicros += duration;
    }
  }

  @Override
  public void startAndEnd(E category) {
    synchronized(lock) {      
      StatsPair pair = categoryStats.get(category);
      pair.count++;
    }
  }

  @Override
  public int getRunning() {
    synchronized(lock) {
      return running;
    }
  }

  @Override
  public long getCompletedCount(E category) {
    StatsPair statsPair = categoryStats.get(category);
    synchronized(lock) {      
      return statsPair.count;
    }
  }

  @Override
  public long getTotalDurationMicros(E category) {
    StatsPair statsPair = categoryStats.get(category);
    synchronized(lock) {      
      return statsPair.totalDurationMicros;
    }
  }

  @Override
  public ActionStatsMap<E> getAll() {    
    synchronized(lock) {  
      ActionStatsMap<E> stats = new ActionStatsMap<E>(categoriesEnumClass);
      stats.setRunningCount(getRunning());
      for (Map.Entry<E, StatsPair> entry : categoryStats.entrySet()) {
        E category = entry.getKey();
        StatsPair pair = categoryStats.get(category);
        stats.setCategoryStats(entry.getKey(), pair.count, pair.totalDurationMicros);
      }
      return stats;
    }
  }

  @Override
  public void clear() {
    synchronized(lock) {  
      running = 0;
      for (StatsPair pair : categoryStats.values()) {
        pair.count = 0;
        pair.totalDurationMicros = 0;
      }
    }
  }

  // Encapsulates the actions count and total duration
  private static class StatsPair {
    public long count;
    public long totalDurationMicros;
  }
}
