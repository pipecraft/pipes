package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.MutableInt;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe that finds a given percentile p in the input pipe.
 * Each item is mapped to a 'sorting component', specifying the metric by which the order is defined. 
 * This implementation uses O(K) memory, where k is the number of unique sorting-component values. Therefore, it's not safe to use this pipe
 * when K is large, but very efficient when K items can be stored in memory.
 *
 * @param <T> The input items type
 * @param <C> The type of the sorting component. Must be comparable and must be suitable for hash keys.
 *
 * @author Eyal Schneider
 */
public class PercentilePipe<T, C extends Comparable<C>> extends TerminalPipe {
  private final Pipe<T> input;
  private final double p;
  private final Function<T, C> extractor;
  private volatile C percentileValue;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param p The required percentile. Must be between 0.0 and 1.0.
   * @param sortingComponentExtractor Extracts the sorting component from input items. Must not return null. The sorting component but be suitable for hash keys!
   */
  public PercentilePipe(Pipe<T> input, double p, Function<T, C> sortingComponentExtractor) {
    this.input = input;
    if (p < 0.0 || p > 1.0) {
      throw new IllegalArgumentException("Illegal required percentile: " + p);
    }
    this.p = p;
    this.extractor = sortingComponentExtractor;
  }
  
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    HashMap<C, MutableInt> bucketCounts = new HashMap<>(); // Histogram of counts per sorting component
    T next;
    int totalItemsCount = 0;
    while ((next = input.next()) != null) {
      totalItemsCount++;
      C c = extractor.apply(next);
      bucketCounts.computeIfAbsent(c, k -> new MutableInt()).increment();
    }
     
    // No inputs? exit and leave percentileValue == null
    if (bucketCounts.isEmpty()) {
      return;
    }
    
    // Collect all pairs and sort by the sorting component
    List<Entry<C, MutableInt>> pairs = new ArrayList<>(bucketCounts.entrySet());
    pairs.sort(Entry.comparingByKey());
    
    // Scan array to find the right bucket
    int lookupValue = (int) Math.round(p * totalItemsCount);
    int cumulativeItemsCount = 0;
    for (Entry<C, MutableInt> entry : pairs) {
      cumulativeItemsCount += entry.getValue().getValue();
      if (cumulativeItemsCount >= lookupValue) {
        this.percentileValue = entry.getKey();
        break;
      }
    }
  }
  
  /**
   * @return The required percentile value, or null if the input is empty. Call this method after start() completes running.
   */
  public C getPercentileValue() {
    return percentileValue;
  }
}
