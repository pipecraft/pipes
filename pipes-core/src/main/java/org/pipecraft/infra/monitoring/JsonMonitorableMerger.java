package org.pipecraft.infra.monitoring;

import java.util.HashMap;
import java.util.Map;
import net.minidev.json.JSONObject;

/**
 * A simple JsonMonitorable implementation that joins a set of other JsonMonitorables.
 * 
 * @author Eyal Schneider
 */
public class JsonMonitorableMerger implements JsonMonitorable {
  private final JsonMonitorable[] monitorables;

  /**
   * Constructor
   * 
   * @param monitorables The set of monitorables to join. Note that if they have children with the same names, one
   * will override the other in an arbitrarily manner.
   */
  public JsonMonitorableMerger(JsonMonitorable ... monitorables) {
    this.monitorables = monitorables;
  }
  
  @Override
  public JSONObject getOwnMetrics() {
    JSONObject res = new JSONObject();
    for (JsonMonitorable mon : monitorables)
      res.putAll(mon.getOwnMetrics());
      
    return res;
  }

  @Override
  public Map<String, JsonMonitorable> getChildren() {
    HashMap<String, JsonMonitorable> res = new HashMap<>();
    for (JsonMonitorable mon : monitorables)
      res.putAll(mon.getChildren());
    
    return res;
  }

}
