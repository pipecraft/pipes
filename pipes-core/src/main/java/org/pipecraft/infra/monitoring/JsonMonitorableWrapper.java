package org.pipecraft.infra.monitoring;

import java.util.HashMap;
import java.util.Map;
import net.minidev.json.JSONObject;

/**
 * A simple JsonMonitorable implementation that wraps a set of child monitorables and adds own metrics.
 * 
 * @author Eyal Schneider
 */
public class JsonMonitorableWrapper implements JsonMonitorable {
  private final Map<String, ? extends JsonMonitorable> children;
  private final JSONObject ownMetrics;

  public JsonMonitorableWrapper(JSONObject ownMetrics, Map<String, ? extends JsonMonitorable> children) {
    this.ownMetrics = ownMetrics;
    this.children = children;
  }

  public JsonMonitorableWrapper(Map<String, ? extends JsonMonitorable> children) {
    this(new JSONObject(), children);
  }

  public JsonMonitorableWrapper(JSONObject ownMetrics) {
    this(ownMetrics, new HashMap<>());
  }

  @Override
  public JSONObject getOwnMetrics() {
    return new JSONObject(ownMetrics);
  }

  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    return children;
  }
}
