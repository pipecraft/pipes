package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.pipecraft.pipes.exceptions.OutOfOrderPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A base class for pipes performing some set operation on a collection of sorted input pipes.
 * The input pipes must have the same item data type, and must all be ordered by the same order relation.
 * The order relation (comparator) must be consistent with equality (java's Object.equals):
 * a equals b if and only if compare(a,b) == 0.
 * 
 * Input pipes may have item repetitions, but they are always treated as sets, where repetitions
 * make no difference. The output of the pipe is always sorted and includes no duplicates.
 * 
 * @param <T> The data type of the items this pipe works with
 *
 * @author Eyal Schneider
 */
public abstract class MultiSortedBasePipe<T> implements Pipe<T> {
  private final List<? extends Pipe<T>> pipes;
  private final BitSet activePipes;
  private final PriorityQueue<LabeledPipe<T>> queue;
  private final Comparator<? super T> comparator;
  private T next;
  
  /**
   * Constructor
   * 
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   * The order in which the pipes collection is given is used in the shouldOutput(..) method.
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * IMPORTANT NOTE: comparator must be consistent with equals: a.equals(b) if and only if comparator.compare(a,b)==0.
   */
  public MultiSortedBasePipe(List<? extends Pipe<T>> inputPipes, Comparator<? super T> comparator) {
    this.comparator = comparator;
    this.pipes = inputPipes;
    this.activePipes = new BitSet(inputPipes.size());
    queue = new PriorityQueue<>(
        Math.max(inputPipes.size(), 1), // To allow empty list of input pipes 
        (p1,p2) -> comparator.compare(p1.peekNext(), p2.peekNext()));
  }

  /**
   * Constructor
   * 
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * IMPORTANT NOTE: comparator must be consistent with equals: a.equals(b) if and only if comparator.compare(a,b)==0.
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   * The order in which the pipes collection is given is used in the shouldOutput(..) method.
   */
  @SafeVarargs
  public MultiSortedBasePipe(Comparator<? super T> comparator, Pipe<T> ... inputPipes) {
    this(Arrays.asList(inputPipes), comparator);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    for(int i = 0; i < pipes.size(); i++) {
      Pipe<T> pipe = pipes.get(i);
      pipe.start();
      T next = pipe.peek();
      if (next != null) {
        LabeledPipe<T> p = new LabeledPipe<>(pipe, i);
        p.setNext(next);
        queue.add(p);
        activePipes.set(i);
      } 
    }
    if (canTerminate(activePipes)) {
      queue.clear();
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

  /**
   * The method to override for determining whether a visited value should be part of the output or not
   * @param item The item
   * @param pipesSeen A bitset specifying the different input pipes in which the item is found. Index #i corresponds to pipe #i in the pipes list provided in the ctor.
   * Note that it is guaranteed that at least one bit is turned on.
   * @return true for outputting this item, false for skipping
   */
  protected abstract boolean shouldOutput(T item, BitSet pipesSeen);

  /**
   * Called whenever one of the input pipes terminated. Allows implementations to decide whether to exit early if this there's no point in continuing with 
   * the remaining active pipes.  
   * @param activePipes A bitset describing the currently active pipes. Index #i corresponds to pipe #i in the pipes list provided in the ctor.
   * @return true for terminating the output, false for continuing.
   */
  protected abstract boolean canTerminate(BitSet activePipes);

  /**
   * @return The input pipes
   */
  protected List<? extends Pipe<T>> getInputPipes() {
    return pipes;
  }

  private void prepareNext() throws PipeException, InterruptedException {
    LabeledPipe<T> labeledPipe = queue.peek();
    if (labeledPipe == null) {
      next = null;
      return;
    }
    
    T prev = next;
    T item = labeledPipe.peekNext();
    BitSet pipesSeen = new BitSet(pipes.size());
    boolean shouldTerminate = false;
    while (true) {
      while (labeledPipe != null && comparator.compare(labeledPipe.pipe.peek(), item) == 0) { // Continue until the sequence of equal items terminates with either a new item or end of pipe
        pipesSeen.set(labeledPipe.id);
        labeledPipe.pipe.next();
        queue.poll();
        T peeked = labeledPipe.pipe.peek();
        if (peeked != null) {
          labeledPipe.setNext(peeked);
          queue.add(labeledPipe);
        } else {
          activePipes.clear(labeledPipe.id);
          shouldTerminate = canTerminate(activePipes);
        }
        labeledPipe = queue.peek();
      }
      if (shouldTerminate) {
        queue.clear(); // This will cause the pipe output to terminate, after examining the current item
      }
      
      if (prev != null && comparator.compare(item, prev) <= 0) { // Validate sorting of pipes
        throw new OutOfOrderPipeException("One or more of the streams isn't sorted: " + prev + " vs " + item);
      }
      if (shouldOutput(item, pipesSeen)) {
        next = item;
        break;
      } else {
        if (labeledPipe == null) {
          next = null;
          break; 
        } else { // Continue looping on next item in the pipe
          pipesSeen.clear();
          prev = item;
          item = labeledPipe.pipe.peek();
        }
      }
    }
  }

  // A wrapper on a pipe that attaches an the pipe index (defined by the list provided in the ctor), and the next pipe value (for optimization purposes).
  private static class LabeledPipe<C> {
    private final Pipe<C> pipe;
    private final int id;
    private C next;
    
    public LabeledPipe(Pipe<C> pipe, int id) {
      this.pipe = pipe;
      this.id = id;
    }
    
    public void setNext(C next) {
      this.next = next;
    }
    
    public C peekNext() {
      return next;
    }
  }
}
