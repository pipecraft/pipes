package org.pipecraft.infra.monitoring.collectors;

import org.pipecraft.infra.monitoring.JsonMonitorable;

/**
 * A common interface to all action statistics collectors.
 * All collectors must be thread safe. However, they can choose to compromise data consistency for performance reasons. 
 * 
 * @author Eyal Schneider
 *
 * @param <E> The enum describing the different categories to be tracked
 */
public interface ActionStatsCollector<E extends Enum<E>> extends JsonMonitorable {
  /**
   * Indicates that an action has started. Must be followed by end(category).  
   */
  public void start();

  /**
   * @return The current duration (in microseconds) of the started action.
   * Returns an undefined value if no action has been started yet.  
   */
  public long duration();

  /**
   * Indicates that an action has terminated.
   * @param category The category to place the action stats into
   * @return The duration, in microseconds, of the action
   */
  public long end(E category);
  
  /**
   * Indicates the starting and termination of an "instant" action (duration = 0)
   * @param category The category to place the action stats into
   */
  public void startAndEnd(E category);
  
  /**
   * @return The number of actions currently in execution
   */
  public int getRunning();
  
  /**
   * @param category A stats category
   * @return The number of actions of the given category that has been completed so far
   */
  public long getCompletedCount(E category);
  
  /**
   * @param category A stats category
   * @return The total duration of completed actions in the given category
   */
  public long getTotalDurationMicros(E category);
  
  /**
   * @return A snapshot of all statistics. The data consistency guarantees depend on the specific implementation. 
   */
  public ActionStatsMap<E> getAll();

  /**
   * Clears all statistics. The atomicity of this operation depends on the specific implementation.
   */
  public void clear();
}
