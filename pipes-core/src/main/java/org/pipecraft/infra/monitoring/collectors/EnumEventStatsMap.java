package org.pipecraft.infra.monitoring.collectors;

import java.util.EnumMap;

/**
 * 
 * An EventStatsMap based on an enum
 * 
 * @author Eyal Schneider
 *
 * @param <T> The enum type representing the legal category values
 */
public class EnumEventStatsMap <T extends Enum<T>> extends EventStatsMap<T>{

  /**
   * Constructor
   * 
   * @param enumType The class of the enum representing the legal category values
   */
  public EnumEventStatsMap(Class<T> enumType) {
    super(new EnumMap<T, Long>(enumType));
  }
}
