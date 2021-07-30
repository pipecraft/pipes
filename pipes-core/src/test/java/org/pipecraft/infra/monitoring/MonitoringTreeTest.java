package org.pipecraft.infra.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.junit.jupiter.api.Test;

public class MonitoringTreeTest {
  private static final MonitoringTree tree;
  
  ///////////////////////////////////////////
  // Builds the following tree:
  //  ROOT
  //    A1 (u)
  //      B1 (v)
  //        C1 (x)
  //
  //    A2 (w)
  //      B1 (t)
  //        C1 (y)
  //        C2 (z)
  ////////////////////////////////////////////
  static {
    DummyMonitorable a1_b1_c1 = new DummyMonitorable("x");
    DummyMonitorable a2_b1_c1 = new DummyMonitorable("y");
    DummyMonitorable a2_b1_c2 = new DummyMonitorable("z");
    HashMap<String, JsonMonitorable> children = new HashMap<String, JsonMonitorable>();
    children.put("C1", a2_b1_c1);
    children.put("C2", a2_b1_c2);
    DummyMonitorable a2_b1 = new DummyMonitorable("t", children);
    children = new HashMap<String, JsonMonitorable>();
    children.put("C1", a1_b1_c1);
    DummyMonitorable a1_b1 = new DummyMonitorable("v", children);    
    children = new HashMap<String, JsonMonitorable>();
    children.put("B1", a1_b1);
    DummyMonitorable a1 = new DummyMonitorable("u", children);    
    children = new HashMap<String, JsonMonitorable>();
    children.put("B1", a2_b1);
    DummyMonitorable a2 = new DummyMonitorable("w", children);        
    
    tree = new MonitoringTree();
    tree.addRoot("A1", a1);
    tree.addRoot("A2", a2);
  }

  // Build the corresponding json to compare with
  private static final Map<String, Object> expectedJson;
  static {
    // Build map to compare with
    HashMap<String, Object> expected_a1_b1_c1 = new HashMap<String, Object>();
    expected_a1_b1_c1.put("name", "x");
    HashMap<String, Object> expected_a2_b1_c1 = new HashMap<String, Object>();
    expected_a2_b1_c1.put("name", "y");
    HashMap<String, Object> expected_a2_b1_c2 = new HashMap<String, Object>();
    expected_a2_b1_c2.put("name", "z");
    HashMap<String, Object> expected_a2_b1 = new HashMap<String, Object>();
    expected_a2_b1.put("name", "t");
    expected_a2_b1.put("C1", expected_a2_b1_c1);
    expected_a2_b1.put("C2", expected_a2_b1_c2);
    HashMap<String, Object> expected_a1_b1 = new HashMap<String, Object>();
    expected_a1_b1.put("name", "v");
    expected_a1_b1.put("C1", expected_a1_b1_c1);
    HashMap<String, Object> expected_a1 = new HashMap<String, Object>();
    expected_a1.put("name", "u");
    expected_a1.put("B1", expected_a1_b1); 
    HashMap<String, Object> expected_a2 = new HashMap<String, Object>();
    expected_a2.put("name", "w");
    expected_a2.put("B1", expected_a2_b1); 
    
    expectedJson = new HashMap<String, Object>();
    expectedJson.put("A1", expected_a1);
    expectedJson.put("A2", expected_a2);
  }
  
  @Test
  public void testEmpty() {
    MonitoringTree tree = new MonitoringTree();
    JSONObject full = tree.getAll();
    assertTrue(full.isEmpty());
    assertTrue(tree.get("A1").isEmpty());
    assertTrue(tree.get("A1+.B1").isEmpty());
  }
  
  @Test
  public void testFullSimplePath() {
    JSONObject dca = tree.get("A1.B1.C1");
    assertEquals(1, dca.size());
    assertEquals("x", dca.get("name"));

    dca = tree.get("A1.B1.C1!");
    assertEquals(1, dca.size());
    assertEquals("x", dca.get("name"));

    JSONObject dcb = tree.get("A2.B1.C1");
    assertEquals(1, dcb.size());
    assertEquals("y", dcb.get("name"));

    JSONObject json = tree.get("A2.B1.X"); //Non-existing
    assertTrue(json.isEmpty());
  }

  @Test
  public void testPartialSimplePath() {
    JSONObject json = tree.get("");
    assertTrue(deepMapEquals(expectedJson, json));
    json = tree.get("A1");
    assertTrue(deepMapEquals((Map<?,?>)expectedJson.get("A1"), json));
    json = tree.get("A2");
    assertTrue(deepMapEquals((Map<?,?>)expectedJson.get("A2"), json));
    
    json = tree.get("A1.B1");
    assertTrue(deepMapEquals((Map<?,?>)((Map<?,?>)expectedJson.get("A1")).get("B1"), json));
    
    json = tree.get("A2.B1");
    assertTrue(deepMapEquals((Map<?,?>)((Map<?,?>)expectedJson.get("A2")).get("B1"), json));

    json = tree.get("A1.B1.X"); // Non existing
    assertTrue(json.isEmpty());
  }

  @Test
  public void testSpecialTokens() {
    JSONObject json = tree.get("*");
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*+");
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*+.B1");
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*+.*");
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*+.*+.*");
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*.*.C1");
    JSONObject expectedRoot = (JSONObject) JSONValue.parse("{\"A1\":{\"B1\":{\"C1\":{\"name\":\"x\"}}}, \"A2\":{\"B1\":{\"C1\":{\"name\":\"y\"}}}}");
    assertTrue(deepMapEquals(expectedRoot, json));
    json = tree.get("*.*.C1+"); //Last '+' should have no effect on output
    assertTrue(deepMapEquals(expectedRoot, json));
    
    json = tree.get("*.*+.C1");
    expectedRoot = (JSONObject) JSONValue.parse("{\"A1\":{\"B1\":{\"name\":\"v\", \"C1\":{\"name\":\"x\"}}}, \"A2\":{\"B1\":{\"name\":\"t\", \"C1\":{\"name\":\"y\"}}}}");
    assertTrue(deepMapEquals(expectedRoot, json));
  }

  @Test
  public void testExcludeSet() {
    JSONObject json = tree.get("*", Sets.newHashSet("X1","X2","C3")); //Filters nothing
    assertTrue(deepMapEquals(expectedJson, json));

    json = tree.get("*", Sets.newHashSet("C1")); 
    JSONObject expectedRoot = (JSONObject) JSONValue.parse("{\"A1\":{\"name\":\"u\",\"B1\":{\"name\":\"v\"}}, \"A2\":{\"name\":\"w\", \"B1\":{\"name\":\"t\", \"C2\":{\"name\":\"z\"}}}}");
    assertTrue(deepMapEquals(expectedRoot, json));
    
    json = tree.get("*", Sets.newHashSet("A1", "B1")); 
    expectedRoot = (JSONObject) JSONValue.parse("{\"A2\":{\"name\":\"w\"}}");
    assertTrue(deepMapEquals(expectedRoot, json));

  }

  private static class DummyMonitorable extends JsonMonitorableWrapper {
    public DummyMonitorable(String nameAttr, Map<String, ? extends JsonMonitorable> children) {
      super(wrapJson(nameAttr), children);
    }

    public DummyMonitorable(String nameAttr) {
      super(wrapJson(nameAttr));
    }

    public DummyMonitorable(Map<String, ? extends JsonMonitorable> children) {
      super(children);
    }
    
    private static JSONObject wrapJson(String nameAttr) {
      JSONObject res = new JSONObject();
      res.put("name", nameAttr);
      return res;
    }
  }
  
  private static boolean deepMapEquals(Map<?,?> m1, Map<?,?> m2) {
    if (m1.size() != m2.size())
      return false;
    
    for (Map.Entry<?, ?> entry : m1.entrySet()) {
      Object key = entry.getKey();
      Object m1Value = m1.get(key);
      Object m2Value = m2.get(key);
      if (m1Value instanceof Map<?,?> && m2Value instanceof Map<?,?>)
        if (!deepMapEquals((Map<?,?>)m1Value, (Map<?,?>)m2Value))
          return false;
      
      if ((m1Value == null) != (m2Value == null))
        return false;

      
      if (m1Value != null && !m1Value.equals(m2Value))
        return false;
    }
    
    return true;
  }
}
