package org.pipecraft.infra.monitoring.sampling;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * A utility for sampling of recent values, using a blocking approach for guaranteeing k latest events in a FIFO order.
 * 
 * @param <T> The type of the values to be sampled.
 *
 * @author Eyal Schneider
 */
public class BlockingSampler<T> implements Sampler<T> {
  private final T[] arr;
  private int pos; 
  private final SampleTextualizer<T> textualizer;
  private final Object lock = new Object();
  
  /**
   * Constructor
   *  
   * @param size The number of sampled values to keep
   * @param textualizer The textualizer to use for converting sample items into text for monitoring purposes
   */
  @SuppressWarnings("unchecked")
  public BlockingSampler(int size, SampleTextualizer<T> textualizer) {
    this.arr = (T[]) new Object[size];
    this.textualizer = textualizer;
  }

  /**
   * Constructor
   *  
   * @param size The number of sampled values to keep
   */
  public BlockingSampler(int size) {
    this(size, new SimpleSampleTextualizer<T>());
  }

  /**
   * Notifies about a new value
   * @param value The new value. 
   */
  public boolean newValue(T value) {
    synchronized(lock) {
      int newPos = (pos + 1) % arr.length;
      arr[newPos] = value;
      pos++;
    }
    return true;
  }
  
  /**
   * @return The collection of all values currently stored in the sampler. 
   * The collection is ordered from latest even to oldest, and no event in this range is skipped.
   */
  public Collection<T> getSnapshot() {
    synchronized(lock) {
      ArrayList<T> res = new ArrayList<T>(arr.length);
      for(int i = 0; i < arr.length; i++) {
        int ind = (pos - i + arr.length) % arr.length;
        T value = arr[ind];
        res.add(value);
      }
      return res;
    }
  }

  @Override
  public JSONObject getOwnMetrics() {
    Collection<T> snapshot = getSnapshot();
    JSONObject res = new JSONObject();
    JSONArray jsonArr = new JSONArray();
    for(T value : snapshot)
      jsonArr.add(textualizer.toText(value));
    res.put("items", jsonArr);
    return res;
  }

  @Override
  public Map<String, JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }

  /**
   * @return The capacity of the sampler
   */
  public int size() {
    return arr.length;
  }
}