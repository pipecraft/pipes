package org.pipecraft.infra.monitoring.sampling;

/**
 * Converts sample items into a text representation
 *
 * @param <T> The sample item type
 *
 * @author Eyal Schneider
 */
public interface SampleTextualizer<T> {
  /**
   * @param obj An item from the sample. Can be null.
   * @return The string representation of the sample item
   */
  public String toText(T obj);
}
