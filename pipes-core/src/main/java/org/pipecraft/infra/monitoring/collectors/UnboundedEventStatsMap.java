package org.pipecraft.infra.monitoring.collectors;

import java.util.HashMap;

/**
 * 
 * An EventStatsMap with no predefined set of possible categories.
 *
 * @param <T> The type representing the legal categories. Must be suitable as a hash structure key.
 *
 * @author Eyal Schneider
 */
public class UnboundedEventStatsMap <T> extends EventStatsMap<T>{

  /**
   * Constructor
   */
  public UnboundedEventStatsMap() {
    super(new HashMap<T, Long>());
  }
}
