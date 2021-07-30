package org.pipecraft.infra.monitoring;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import net.minidev.json.JSONObject;

/**
 * A hierarchy of JsonMonitorable objects.
 * Allows retrieving the global json representation, or a json representation for a specific entity in the tree.
 * 
 * Thread safe.
 * 
 * @author Eyal Schneider
 */
public class MonitoringTree implements JsonMonitorable {
  private ConcurrentHashMap<String, JsonMonitorable> roots = new ConcurrentHashMap<String, JsonMonitorable>();
  
  /**
   * Registers a new top level monitored entity.
   * @param id The identifier of the monitorable entity. Must not include the following chars: '.', '+', '*', since they have a speciall role in the lookup method.
   * @param monitorable The monitorable object. The contents of monitorable.getChildren() determines the sub-tree being added.
   */
  public void addRoot(String id, JsonMonitorable monitorable) {
    roots.put(id, monitorable);
  }
  
  /**
   * @param path A path, composed of zero or more dot separated tokens (empty string refers to the root entity), and possibly 
   * ending with "!".
   * A token may be simple or special. A simple token is an identifier of a nested monitored entity.
   * A special token is one of "*", "*+" or "{id}+".
   * When these forms are used, the returned json object will maintain the full tree structure, including the root.
   * "*" is used for including all children in some level in the output. "+" is used for including the monitorable's own metrics in that level of the output.
   * 
   * When the path ends with "!", it truncates all child nodes of the nodes represented by the last token of the path.
   * 
   * @param excludeSet A set of node names to exclude completely from the output (including their descendants)
   * @return The json representation of the selected entity set. Note that the method is tolerant to non existing paths, and collects only the data that could be found.
   * Null is never returned.
   */
  public JSONObject get(String path, Set<String> excludeSet) {
    JSONObject res;
    
    if (path.contains("*") || path.contains("+")) {
      res = new JSONObject();
      get(this, res, path, false, false, excludeSet);
    } else {
      res = getSimple(this, path, excludeSet);
    }
    
    return res;
  }

  private JSONObject getSimple(JsonMonitorable mon, String path, Set<String> excludeSet) {
    StringTokenizer st = new StringTokenizer(path, ".");
    boolean truncate = false;
    while (st.hasMoreTokens()) {
      String token = st.nextToken();
      
      if (excludeSet.contains(token)) {
        return new JSONObject();
      }
      if (token.endsWith("!")) {
        truncate = true;
        token = token.substring(0, token.length() - 1);
      }
      Map<String, ? extends JsonMonitorable> children = mon.getChildren();
      mon = children.get(token);
      if (mon == null) {
        return new JSONObject();
      }
      if (truncate) {
        break;
      }
    }
    
    if (truncate) {
      return mon.getOwnMetrics();
    } else {
      return getAsJson(mon);
    }
  }

  /**
   * @param path A path, composed of zero or more dot separated tokens (empty string refers to the root entity), and possibly 
   * ending with "!".
   * A token may be simple or special. A simple token is an identifier of a nested monitored entity.
   * A special token is one of "*", "*+" or "{id}+".
   * When these forms are used, the returned json object will maintain the full tree structure, including the root.
   * "*" is used for including all children in some level in the output. "+" is used for including the monitorable's own metrics in that level of the output.
   * 
   * When the path ends with "!", it truncates all child nodes of the nodes represented by the last token of the path.
   * 
   * @return The json representation of the selected entity set. Note that the method is tolerant to non existing paths, and collects only the data that could be found.
   * Null is never returned.
   */
  public JSONObject get(String path) {
    return get(path, Collections.emptySet());
  }
  
  /**
   * @return The json representation of the complete tree
   */
  public JSONObject getAll() {
    return getAsJson(this);
  }

  /**
   * @return A json tree with null values as leaves, that represents the structure that can be queried. Any path in this tree
   * can be queried using get(path). 
   */
  public JSONObject getInventory() {
    return getInventory(this);
  }

  /**
   * @param monitorable A monitorable entity
   * @return The full json representation of the entity (including all descendants)
   */
  public static JSONObject getAsJson(JsonMonitorable monitorable) {
    JSONObject res = monitorable.getOwnMetrics();
    
    for (Map.Entry<String, ? extends JsonMonitorable> entry : monitorable.getChildren().entrySet())
      res.put(entry.getKey(), getAsJson(entry.getValue()));
    
    return res;
  }
  
  /**
   * @param monitorable A monitorable entity
   * @return A json tree with null values as leaves, that represents the structure that can be queried. Any path in this tree
   * can be queried using get(path). 
   */
  public static JSONObject getInventory(JsonMonitorable monitorable) {
    Map<String, ? extends JsonMonitorable> children = monitorable.getChildren();
    if (children.isEmpty())
      return null;
    
    JSONObject res = new JSONObject();
    for (Map.Entry<String, ? extends JsonMonitorable> entry : children.entrySet())
      res.put(entry.getKey(), getInventory(entry.getValue()));
    
    return res;
    
  }

  @Override
  public JSONObject getOwnMetrics() {
    return new JSONObject();
  }

  @Override
  public Map<String, JsonMonitorable> getChildren() {
    return roots;
  }
  
  /**
   * Populates a given json object recursively, according to the given path specifications.
   * Always populates full path (i.e. a.b.c will result in a path a.b.c where c is fully populated.
   * Repsects special path spec commands (*, !, +).
   * 
   * @param monitorable The input monitorable
   * @param outputNode The output node
   * @param pathSpec The path specifications
   * @param includeOwnMetrics Should the current node include its own data
   * @param truncate No more info should be added under the current node.
   * @param excludeSet A set of names of nodes to exclude from the output (including descendants)
   */
  private void get(JsonMonitorable monitorable, JSONObject outputNode, String pathSpec, boolean includeOwnMetrics, boolean truncate, Set<String> excludeSet) {
    // Extract next token in path
    int dotInd = pathSpec.indexOf('.');
    String pathSuffix;
    String token;
    if (dotInd != -1) {
      token = pathSpec.substring(0, dotInd);
      pathSuffix = pathSpec.substring(dotInd + 1);
    } else {      
      token = pathSpec;
      pathSuffix = "";
    }
    
    // Add own metrics to output
    if (includeOwnMetrics)
      outputNode.putAll(monitorable.getOwnMetrics());

    // Handle truncation
    if (truncate) {
      return;
    }
    truncate = token.endsWith("!");
    if (truncate)
      token = token.substring(0, token.length() - 1);

    // Look for "+" suffix and update includeOwnMetrics for the next level
    includeOwnMetrics = token.endsWith("+");
    if (includeOwnMetrics)
      token = token.substring(0, token.length() - 1);
    includeOwnMetrics |= pathSuffix.isEmpty();
    
    if (pathSuffix.isEmpty()) {
      pathSuffix = "*+";
    }

    // Handle "*" wildcard by extracting all children
    if (token.equals("*")) {
      for (Map.Entry<String,? extends JsonMonitorable> childEntry : monitorable.getChildren().entrySet()) {
        JsonMonitorable child = childEntry.getValue();
        
        JSONObject childNode = new JSONObject();
        String childName = childEntry.getKey();
        if (!excludeSet.contains(childName)) {
          outputNode.put(childName, childNode);
          get(child, childNode, pathSuffix, includeOwnMetrics, truncate, excludeSet);
        }
      }
    } else {
      JsonMonitorable child = monitorable.getChildren().get(token);
      if (child == null || excludeSet.contains(token))
        return;
      
      JSONObject childNode = new JSONObject();
      outputNode.put(token, childNode);
      outputNode = childNode;        
            
      get(child, outputNode, pathSuffix, includeOwnMetrics, truncate, excludeSet);
    }
  }
}
