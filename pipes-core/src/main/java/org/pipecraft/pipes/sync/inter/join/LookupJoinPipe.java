package org.pipecraft.pipes.sync.inter.join;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * A pipe performing a join operation between a 'left' pipe of type L, and a list of 'right' pipes of type R.
 * This join is only relevant when we can guarantee that the contents of all the right side pipes can fit entirely in memory.
 * Under these conditions, this pipe will be much more efficient than the {@link HashJoinPipe} alternative, which makes use of temporary disk space.
 *
 * Note that this pipe assumes that the left pipe has unique entries (with respect to the join key). In case there are repetitions the join will still work,
 * but will not conform with the standard join semantics of other join pipes ({@link SortedJoinPipe} and {@link HashJoinPipe}), where
 * each join record covers to all key matches from left side and right side. The current pipe will always produce at most one left side match in each join record.
 *
 * No specific item order in required in the input pipes.
 * 
 * The output type for this pipe is {@link JoinRecord}, which consists of the key, the left matches and the right matches.
 * 
 * The join can work in LEFT/INNER/FULL_INNER/OUTER mode. See {@link JoinMode} for more details.
 *
 * @param <K> The type of the key used for matching records. Must be suitable to serve at a hash based data structure key.
 * @param <L> The type of left side records
 * @param <R> The type of right side records
 *
 * @author Eyal Schneider
 */
public class LookupJoinPipe<K, L, R> implements Pipe<JoinRecord<K, L, R>> {
  private final Pipe<L> leftPipe;
  private final FailableFunction<L, K, PipeException> leftKeyExtractor;
  private final List<? extends Pipe<R>> rightPipes;
  private final FailableFunction<R, K, PipeException> rightKeyExtractor;
  private final JoinMode joinMode;
  private HashMap<K, RightSideMatches<R>> rightLookup; // Maps a key to an array where array index i corresponds to right pipe #i, and subindex j corresponds to match #j found in the same pipe #i.
  private Iterator<Entry<K, RightSideMatches<R>>> rightIter;
  private JoinRecord<K, L, R> next;

  /**
   * Constructor
   *
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   */
  public LookupJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor,
      JoinMode joinMode) {
    this.leftPipe = leftPipe;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightPipes = rightPipes;
    this.rightKeyExtractor = rightKeyExtractor;
    this.joinMode = joinMode;
  }

  /**
   * Constructor
   *
   * To be used when there's a single right pipe.
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipe The right side pipe
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   */
  public LookupJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, Pipe<R> rightPipe, FailableFunction<R, K, PipeException> rightKeyExtractor,
      JoinMode joinMode) {
    this(leftPipe, leftKeyExtractor, Collections.singletonList(rightPipe), rightKeyExtractor, joinMode);
  }

  /**
   * Constructor
   *
   * A constructor for the case of no left pipe. Assumes join type OUTER among the right pipes.
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   */
  public LookupJoinPipe(List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor) {
    this(EmptyPipe.instance(), v -> null, rightPipes, rightKeyExtractor, JoinMode.OUTER);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    // Build lookup using right side pipes data
    rightLookup = new HashMap<>();
    for (int pipeInd = 0; pipeInd < rightPipes.size(); pipeInd++) {
      Pipe<R> rPipe = rightPipes.get(pipeInd);
      rPipe.start();
      R rNext;
      while ((rNext = rPipe.next()) != null) {
        K key = rightKeyExtractor.apply(rNext);
        RightSideMatches<R> rMatches = rightLookup.computeIfAbsent(key, k -> new RightSideMatches<>(rightPipes.size()));
        List<R> l = rMatches.matches[pipeInd];
        if (l == null) {
          l = new ArrayList<>();
          rMatches.matches[pipeInd] = l;
        }
        l.add(rNext);
      }
    }

    leftPipe.start();
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

  private void prepareNext() throws PipeException, InterruptedException {
    L lNext = leftPipe.next();
    if (lNext == null) {
      if (joinMode == JoinMode.OUTER) { // if we are done with left side and join mode is also interested in pure right side matches, we should use an iterator on right side items in the map
        if (rightIter == null) {
          rightIter = rightLookup.entrySet().iterator();
        }
        while (rightIter.hasNext()) {
          Entry<K, RightSideMatches<R>> entry = rightIter.next();
          RightSideMatches<R> rMatches = entry.getValue();
          if (!rMatches.visited) { // We want to skip keys for which we already found matches with left side
            JoinRecord<K, L, R> joinRec = new JoinRecord<>(entry.getKey());
            for (int id = 0; id < rightPipes.size(); id++) {
              List<R> pipeMatches = rMatches.matches[id];
              if (pipeMatches != null) {
                for (R item : pipeMatches) {
                  joinRec.addRight(id, item);
                }
              }
            }
            if (joinMode.shouldOutput(joinRec, rightPipes.size())) {
              next = joinRec;
              return;
            }
          }
        }
      }
    } else {
      do {
        K key = leftKeyExtractor.apply(lNext);
        JoinRecord<K, L, R> joinRec = new JoinRecord<>(key);
        joinRec.addLeft(lNext);
        RightSideMatches<R> right = rightLookup.get(key);
        if (right != null) {
          right.visited = true;
          for (int pipeInd = 0; pipeInd < right.matches.length; pipeInd++) {
            List<R> pipeMatches = right.matches[pipeInd];
            if (pipeMatches != null) {
              for (R rMatch : pipeMatches) {
                joinRec.addRight(pipeInd, rMatch);
              }
            }
          }
        }
        if (joinMode.shouldOutput(joinRec, rightPipes.size())) {
          next = joinRec;
          return;
        }
      } while ((lNext = leftPipe.next()) != null);
    }
    next = null;
  }

  @Override
  public float getProgress() {
    return joinMode.resolveProgress(leftPipe, rightPipes);
  }

  private static class RightSideMatches <R> {
    private final List<R>[] matches;
    private boolean visited;

    @SuppressWarnings("unchecked")
    public RightSideMatches(int size) {
      this.matches = new List[size];
    }
  }
}
