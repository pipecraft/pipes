package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.Map;
import net.minidev.json.JSONObject;


/**
 * A baseline for statistics collector implementations.
 *
 * @author Eyal Schneider
 *
 * @param <E> The enum describing the different categories to be tracked
 */
public abstract class AbstractActionStatsCollector<E extends Enum<E>> implements ActionStatsCollector<E>{
  private final ThreadLocal<Long> actionTrackingTL = new ThreadLocal<>();

  protected final Class<E> categoriesEnumClass;

  /**
   * Constructor
   * @param categoriesEnumClass the enum describing the different categories to be tracked
   */
  public AbstractActionStatsCollector(Class<E> categoriesEnumClass) {
    this.categoriesEnumClass = categoriesEnumClass;
  }

  @Override
  public void start() {
    actionTrackingTL.set(System.nanoTime());
    innerStart();
  }

  @Override
  public long duration() {
    Long startTime = actionTrackingTL.get();
    return calcDuration(startTime, System.nanoTime());
  }

  @Override
  public long end(E category) {
    Long startTime = actionTrackingTL.get();
    if (startTime == null)
      throw new IllegalStateException("end(..) called without a previous call to start()");

    return end(category, startTime);
  }

  public long end(E category, long startTime) {
    long currTime = System.nanoTime();
    long duration = calcDuration(startTime, currTime);
    innerEnd(category, duration);
    actionTrackingTL.set(null);
    return duration;
  }

  /**
   * The specific implementation of the "start" operation
   */
  protected abstract void innerStart();

  /**
   * The specific implementation of the "end" operation
   * @param category The action category
   * @param duration The measured action duration, in micros
   */
  protected abstract void innerEnd(E category, long duration);

  @Override
  public JSONObject getOwnMetrics() {
    return getAll().getOwnMetrics();
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    return getAll().getChildren();
  }

  /**
   * @param startTime Start time, in nanos
   * @param endTime End time, in nanos
   * @return The duration, in micro-seconds
   */
  private static long calcDuration(long startTime, long endTime) {
    return Math.round((endTime - startTime) / 1000.0);
  }

}
