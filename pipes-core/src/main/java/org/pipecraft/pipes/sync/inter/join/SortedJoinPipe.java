package org.pipecraft.pipes.sync.inter.join;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.OutOfOrderPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.EmptyPipe;

/**
 * A pipe performing a join operation between a 'left' pipe of type L, and a list of 'right' pipes of type R.
 * Both left and right pipes must be ordered by some key, and may have duplicates.
 * 
 * The output type for this pipe is {@link JoinRecord}, which consists of the key, the left matches and the right matches.
 * 
 * The join can work in LEFT/INNER/FULL_INNER/OUTER mode. See {@link JoinMode} for more details.
 * 
 * @param <K> The type of the key used for matching records
 * @param <L> The type of left side records
 * @param <R> The type of right side records
 *
 * @author Eyal Schneider
 */
public class SortedJoinPipe<K, L, R> implements Pipe<JoinRecord<K, L, R>> {
  private final Pipe<L> leftPipe;
  private final FailableFunction<L, K, PipeException> leftKeyExtractor;
  private final List<? extends Pipe<R>> rightPipes;
  private final FailableFunction<R, K, PipeException> rightKeyExtractor;
  private PriorityQueue<LabeledPipe<K, ?>> queue;
  private final Comparator<? super K> keyComparator;
  private final JoinMode joinMode;
  private final BitSet activePipes;
  private JoinRecord<K, L, R> next;
  private boolean earlyExit;

  /**
   * Constructor
   * 
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param keyComparator The total order definition to use when merging the data from the different input pipes.
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   */
  public SortedJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor,
      Comparator<? super K> keyComparator, JoinMode joinMode) {
    this.leftPipe = leftPipe;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightPipes = rightPipes;
    this.rightKeyExtractor = rightKeyExtractor;
    this.keyComparator = keyComparator;
    this.joinMode = joinMode;
    this.activePipes = new BitSet(1 + rightPipes.size());
  }

  /**
   * Constructor
   * 
   * To be used when there's a single right pipe.
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipe The right side pipe 
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param keyComparator The total order definition to use when merging the data from the different input pipes.
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   */
  public SortedJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, Pipe<R> rightPipe, FailableFunction<R, K, PipeException> rightKeyExtractor,
      Comparator<? super K> keyComparator, JoinMode joinMode) {
    this(leftPipe, leftKeyExtractor, Collections.singletonList(rightPipe), rightKeyExtractor, keyComparator, joinMode);
  }

  /**
   * Constructor
   *
   * A constructor for the case of no left pipe. Assumes join type OUTER among the right pipes.
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param keyComparator The total order definition to use when merging the data from the different input pipes.
   */
  public SortedJoinPipe(List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor, Comparator<? super K> keyComparator) {
    this(EmptyPipe.instance(), v -> null, rightPipes, rightKeyExtractor, keyComparator, JoinMode.OUTER);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    queue = new PriorityQueue<>(
        1 + rightPipes.size(),  
        (lp1,lp2) -> keyComparator.compare(lp1.peekNextKey(), lp2.peekNextKey()));

    leftPipe.start();
    L nextL = leftPipe.peek();
    if (nextL != null) {
      activePipes.set(0);
      LabeledPipe<K, L> p = new LabeledPipe<>(leftPipe, 0);
      p.setNext(nextL, leftKeyExtractor.apply(nextL));
      queue.add(p);
    }
    
    for(int i = 0; i < rightPipes.size(); i++) {
      Pipe<R> rPipe = rightPipes.get(i);
      rPipe.start();
      R next = rPipe.peek();
      if (next != null) {
        final int rightPipeId = i + 1;
        activePipes.set(rightPipeId);
        LabeledPipe<K, R> p = new LabeledPipe<>(rPipe, rightPipeId);
        p.setNext(next, rightKeyExtractor.apply(next));
        queue.add(p);
      }
    }
    prepareNext();
  }
  
  @Override
  public void close() throws IOException {
    IOException lastExc = null;
    
    try {
      leftPipe.close();
    } catch (IOException e) {
      lastExc = e;
    }
    
    for (Pipe<R> p : rightPipes) {
      try {
        p.close();
      } catch (IOException e) {
        lastExc = e;
      }
    }
    if (lastExc != null) {
      throw lastExc;
    }
  }

  @Override
  public JoinRecord<K, L, R> next() throws PipeException, InterruptedException {
    JoinRecord<K, L, R> toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public JoinRecord<K, L, R> peek() {
    return next;
  }

  @SuppressWarnings("unchecked")
  private void prepareNext() throws PipeException, InterruptedException {
    LabeledPipe<K, ?> labeledPipe = queue.peek();
    if (labeledPipe == null) {
      next = null;
      return;
    }
    
    K prevKey = next == null? null : next.getKey();
    K key = labeledPipe.peekNextKey();
    JoinRecord<K, L, R> candidate = new JoinRecord<>(key);
    while (true) {
      while (labeledPipe != null && labeledPipe.peekNextKey().equals(key)) { // Continue until the sequence of equal keys terminates with either a new item or end of data
        // Record the match (either left or right)
        if (labeledPipe.id == 0) {
          candidate.addLeft((L) labeledPipe.peekNext());
        } else {
          candidate.addRight(labeledPipe.id - 1, (R) labeledPipe.peekNext());
        }
        
        // Advance the selected pipe and the queue
        labeledPipe.pipe.next();
        queue.poll();
        Object peeked = labeledPipe.pipe.peek();
        if (peeked != null) {
          labeledPipe.setNext(peeked, labeledPipe.id == 0 ? leftKeyExtractor.apply((L)peeked) : rightKeyExtractor.apply((R)peeked));
          queue.add(labeledPipe);
        } else { // One of input pipes ended
          activePipes.clear(labeledPipe.id);
          if (joinMode.canEarlyExit(activePipes, rightPipes.size())) { // Can we do an early-exit?
            earlyExit = true;
          }
        }
        
        // Fetch next from queue
        labeledPipe = queue.peek();
      }
      if (prevKey != null && keyComparator.compare(key, prevKey) <= 0) { // Validate sorting of pipes
        throw new OutOfOrderPipeException("One or more of the streams isn't sorted: " + prevKey + " vs " + key);
      }
      
      if (joinMode.shouldOutput(candidate, rightPipes.size())) {
        next = candidate;
        break;
      } else {
        if (earlyExit || labeledPipe == null) { // End of data
          queue.clear();
          next = null;
          break; 
        } else { // Continue looping on next item in the pipe
          prevKey = key;
          key = labeledPipe.peekNextKey();
          candidate = new JoinRecord<>(key);
        }
      }
    }
  }

  // A wrapper on a pipe that attaches an index to it, where index 0 corresponds to the left pipe, and the others to the
  // right pipes, according to their order in the ctor.
  private static class LabeledPipe<K, C> {
    private final Pipe<C> pipe;
    private final int id;
    private C next;
    private K nextKey;
    
    public LabeledPipe(Pipe<C> pipe, int id) {
      this.pipe = pipe;
      this.id = id;
    }
    
    @SuppressWarnings("unchecked")
    public void setNext(Object next, K nextKey) {
      this.next = (C)next;
      this.nextKey = nextKey;
    }
    
    public K peekNextKey() {
      return nextKey;
    }
    
    public C peekNext() {
      return next;
    }
  }

  @Override
  public float getProgress() {
    return joinMode.resolveProgress(leftPipe, rightPipes);
  }

}
