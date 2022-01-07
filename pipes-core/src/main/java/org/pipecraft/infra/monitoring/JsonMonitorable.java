package org.pipecraft.infra.monitoring;

import java.util.Collections;
import java.util.Map;

import net.minidev.json.JSONObject;

/**
 * To be implemented by classes that can "export" their state (or part of it) as Json.
 * Supports hierarchy.
 * 
 * @author Eyal Schneider
 */
public interface JsonMonitorable {
  /**
   * @return The json string describing the entity, not including the child entities.  
   */
  JSONObject getOwnMetrics();
  
  /**
   * @return The monitorable children of this entity, as [id, JsonExportable] pairs.
   * Using this method, the framework can manage a complete tree to be monitored, where every entity has a unique path.
   */
  default Map<String, ? extends JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }
  
  /**
   * @return The json representation of this monitorable, including own metrics and children
   */
  default JSONObject getFullMetrics() {
    return MonitoringTree.getAsJson(this);
  }
}
