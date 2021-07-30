package org.pipecraft.infra.monitoring.sampling;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * A utility for sampling of recent values, using a non-blocking approach for improved performance.
 * A capacity of K doesn't guarantee that last K values are stored, though it is highly probable, assuming that threads aren't likely
 * to perform the sample operation at the exact same time.
 * 
 * What this implementation does guarantee, is that the K items stored in the sample are a subset of the last
 * K*Min(P,T) calls to the sampler, where T stands for the number of threads performing sampling, and P stands for the number of cores.
 * 
 * @param <T> The type of the values to be sampled.
 *
 * @author Eyal Schneider
 */
public class InaccurateSampler<T> implements Sampler<T> {
  private final AtomicReferenceArray<T> arr;
  private final AtomicInteger pos = new AtomicInteger(); 
  private final SampleTextualizer<T> textualizer;
  
  /**
   * Constructor
   *  
   * @param size The number of sampled values to keep
   * @param textualizer The textualizer to use for converting sample items into text for monitoring purposes
   */
  public InaccurateSampler(int size, SampleTextualizer<T> textualizer) {
    this.arr = new AtomicReferenceArray<T>(size);
    this.textualizer = textualizer;
  }

  /**
   * Constructor
   *  
   * @param size The number of sampled values to keep
   */
  public InaccurateSampler(int size) {
    this(size, new SimpleSampleTextualizer<T>());
  }

  @Override
  public boolean newValue(T value) {
    int currPos = pos.get();
    int reservedPos = (currPos + 1) % arr.length();
    if (pos.compareAndSet(currPos, reservedPos)) {
      arr.set(reservedPos, value);
      return true;
    }
    return false;
  }
  
  /**
   * @return The collection of all values currently stored in the sampler. 
   * Consistency is not guaranteed (i.e. this is not necessarily a real snapshot of the inner collection).
   * Note that null values or unrecorded values are returned as nulls.
   */
  public Collection<T> getSnapshot() {
    ArrayList<T> res = new ArrayList<T>(arr.length());
    for(int i = 0; i < arr.length(); i++) {
      T value = arr.get(i);
      res.add(value);
    }
    return res;
  }

  @Override
  public JSONObject getOwnMetrics() {
    JSONObject res = new JSONObject();
    JSONArray jsonArr = new JSONArray();
    for(int i = 0; i < arr.length(); i++) {
      T value = arr.get(i);
      jsonArr.add(textualizer.toText(value));
    }  
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
    return arr.length();
  }
}