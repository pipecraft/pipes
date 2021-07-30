package org.pipecraft.infra.monitoring.sampling;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.Collection;

/**
 * Allows sampling values from a stream
 * 
 * @param T The type of the items being sampled
 * @author Eyal Schneider
 */
public interface Sampler<T> extends JsonMonitorable {
  /**
   * Notifies about a new value
   * @param value The new value.
   * @return true iff the new value has been recorded in the sample. 
   */
  public boolean newValue(T value);
  
  /**
   * @return The collection of all values currently stored in the sampler. 
   * Consistency is not guaranteed (i.e. this is not necessarily a real snapshot of the inner collection).
   * Note that null values or unrecorded values are returned as nulls.
   */
  public Collection<T> getSnapshot();
  
  /**
   * @return The capacity of the sampler
   */
  public int size();
  
}
