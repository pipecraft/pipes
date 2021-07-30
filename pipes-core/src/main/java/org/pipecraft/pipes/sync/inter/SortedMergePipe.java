package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.pipecraft.pipes.exceptions.OutOfOrderPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * An intermediate pipe sort-merging the contents of a collection of sorted input pipes.
 * The input pipes must have the same item data type, and must all be ordered by the same order relation.
 * 
 * In contrast to {@link SortedUnionPipe}, the order relation (comparator) used here
 * may not be consistent with item equality.
 * 
 * Input pipes may have item repetitions, which will appear in the output.
 * The output of the pipe is always sorted.
 * 
 * @param <T> The data type of the items this pipe works with
 *
 * @author Eyal Schneider
 */
public class SortedMergePipe<T> implements Pipe<T> {
  private final List<? extends Pipe<T>> pipes;
  private final PriorityQueue<LabeledPipe<T>> queue;
  private final Comparator<? super T> comparator;
  private T next;
  
  /**
   * Constructor
   * 
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   */
  public SortedMergePipe(List<? extends Pipe<T>> inputPipes, Comparator<? super T> comparator) {
    this.comparator = comparator;
    this.pipes = inputPipes;
    queue = new PriorityQueue<>(
        Math.max(inputPipes.size(), 1), // To allow empty list of input pipes 
        (p1,p2) -> comparator.compare(p1.peek(), p2.peek()));
  }

  /**
   * Constructor
   * 
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   */
  @SafeVarargs
  public SortedMergePipe(Comparator<? super T> comparator, Pipe<T> ... inputPipes) {
    this(Arrays.asList(inputPipes), comparator);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    for (Pipe<T> pipe : pipes) {
      pipe.start();
      T next = pipe.peek();
      if (next != null) {
        LabeledPipe<T> p = new LabeledPipe<>(pipe);
        p.fetchNext();
        queue.add(p);
      }
    }
    prepareNext();    
  }
  
  @Override
  public void close() throws IOException {
    FileUtils.close(pipes);
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public T peek() {
    return next;
  }

  private void prepareNext() throws PipeException, InterruptedException {
    LabeledPipe<T> labeledPipe = queue.poll();
    if (labeledPipe == null) {
      next = null;
      return;
    }
        
    T item = labeledPipe.peek();
    labeledPipe.fetchNext();
    if (labeledPipe.peek() != null) {
      queue.add(labeledPipe);
    } 
    
    if (next != null && comparator.compare(item, next) < 0) { // Validate sorting of pipes
      throw new OutOfOrderPipeException("One or more of the streams isn't sorted: " + next + " vs " + item);
    }

    next = item;
  }

  // A wrapper on a pipe that holds the next pipe value (for optimization purposes).
  private static class LabeledPipe<C> {
    private final Pipe<C> pipe;
    private C next;
    
    public LabeledPipe(Pipe<C> pipe) {
      this.pipe = pipe;
    }
    
    public void fetchNext() throws PipeException, InterruptedException {
      this.next = pipe.next();
    }
    
    public C peek() {
      return next;
    }
  }
  
  @Override
  public float getProgress() {
    return PipeUtils.getMinProgress(pipes);
  }
}
