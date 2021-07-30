package org.pipecraft.pipes.sync.inter.join;

import java.util.BitSet;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * The different join modes supported by join pipes
 * 
 * In LEFT mode, all keys from the left pipe (and only them) will be included in the results. 
 * In INNER mode, only keys which appear both in left and any of the right pipes will be included. 
 * In FULL_INNER mode, only keys which appear both in left and all of the right pipes will be included.
 * In OUTER mode, all keys that appear either in left or any of right pipes will be included.
 * 
 * @author Eyal Schneider
 * 
 */
public enum JoinMode {
  LEFT(       (r, c) -> r.getLeft().size() > 0, 
              (l, r) -> l.getProgress(),
              (p, c) -> !p.get(0)),
  INNER(      (r, c) -> r.getLeft().size() > 0 && r.getRight().size() > 0, 
              (l, r) -> Math.max(l.getProgress(), PipeUtils.getMinProgress(r)),
              (p, c) -> !p.get(0) || p.nextSetBit(1) == -1),
  FULL_INNER( (r, c) -> r.getLeft().size() > 0 && r.getRight().size() == c, 
              (l, r) -> Math.max(l.getProgress(), PipeUtils.getMaxProgress(r)),
              (p, c) -> p.cardinality() < c + 1),
  OUTER(      (r, c) -> true, 
              (l, r) -> Math.min(l.getProgress(), PipeUtils.getMinProgress(r)),
              (p, c) -> p.isEmpty());

  private final BiPredicate<JoinRecord<?, ?, ?>, Integer> selector;
  private final BiFunction<Pipe<?>, Collection<? extends Pipe<?>>, Float> progressResolver;
  private final BiPredicate<BitSet, Integer> earlyExitPredicate;
  
  /**
   * Constructor
   * 
   * @param selector A predicate receiving a candidate {@link JoinRecord} and right side pipes count, 
   * and determining whether the record should be passed to output.
   * @param progressResolver A function returning the join progress given the left pipe and right pipes
   * @param earlyExitPredicate A predicate which is given the set of active input pipes and the total number of pipes,
   * and determines whether an early exit is possible.
   */
  private JoinMode(
      BiPredicate<JoinRecord<?, ?, ?>, Integer> selector, 
      BiFunction<Pipe<?>, Collection<? extends Pipe<?>> , Float> progressResolver,
      BiPredicate<BitSet, Integer> earlyExitPredicate) {
    this.selector = selector;
    this.progressResolver = progressResolver;
    this.earlyExitPredicate = earlyExitPredicate;
  }

  /**
   * @param joinRec The candidate join record we wish to check
   * @param rightPipeCount Number of right pipes in the join
   * @return True if and only if the candidate join record should be emitted
   */
  public boolean shouldOutput(JoinRecord<?, ?, ?> joinRec, int rightPipeCount) {
    return selector.test(joinRec, rightPipeCount);
  }
  
  /**
   * @param leftPipe The left pipe in the join operation (assumed to be started)
   * @param rightPipes The right pipes in the join operation (assumed to be started)
   * @return The progress computed by checking the progress of all involved pipes
   */
  public float resolveProgress(Pipe<?> leftPipe, Collection<? extends Pipe<?>> rightPipes) {
    return progressResolver.apply(leftPipe, rightPipes);
  }
  
  /**
   * @param activePipes The active pipes, represented as a bitset, where index i refers
   * to pipe with id=i. Id=0 is reserved for left pipe. 
   * @param rightPipeCount Number of right pipes in the join
   * @return true if and only if the current state of the input pipes for the join indicates
   * that an early exit is possible.
   */
  public boolean canEarlyExit(BitSet activePipes, int rightPipeCount) {
    return earlyExitPredicate.test(activePipes, rightPipeCount);
  }
}
