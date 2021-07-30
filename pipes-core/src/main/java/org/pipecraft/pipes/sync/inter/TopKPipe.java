package org.pipecraft.pipes.sync.inter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe returning the top K items of the input pipe, according to some predefined order relation.
 * The items are emitted in descending order.
 * This implementation uses extra memory storage proportional to the given K, so use with care.
 * 
 * @param <T> The target item data type
 *
 * private volatile Pipe<T> finalOutputPipe;
 */
public class TopKPipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private final PriorityQueue<T> heap;
  private final int k;
  private List<T> results;
  private int pos; // Current cursor on results
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param k The required number of top items to emit. If the input pipe has m<k items, then only m items will be returned.
   * @param comparator The comparator defining an order on the input items, based on which the top k items should be returned
   */
  public TopKPipe(Pipe<T> input, int k, Comparator<? super T> comparator) {
    this.input = input;
    this.heap = new PriorityQueue<>(k, comparator);
    this.k = k;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    T item;
    while((item = input.next()) != null) {
      heap.add(item);
      if (heap.size() > k) {
        heap.poll(); // Remove the lowest
      }
    }
    
    int count = heap.size();
    results = new ArrayList<>(count);
    for(int i = 0; i < count; i++) {
      results.add(null);
    }
    
    for(int i = 0; i < count; i++) {
      results.set(count - i - 1, heap.poll());
    }
  }
  
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    if (pos == results.size()) {
      return null;
    }
    return results.get(pos++);
  }

  @Override
  public T peek() {
    return results.get(pos);
  }

  @Override
  public float getProgress() {
    return 1.0f; // The job is done in the start() method
  }
}
