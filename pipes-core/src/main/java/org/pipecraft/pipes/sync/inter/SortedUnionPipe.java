package org.pipecraft.pipes.sync.inter;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * An intermediate pipe performing set union operation on a collection of sorted input pipes.
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
public class SortedUnionPipe<T> extends MultiSortedBasePipe<T>{

  /**
   * Constructor
   * 
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * IMPORTANT NOTE: comparator must be consistent with equals: a.equals(b) if and only if comparator.compare(a,b)==0.
   */
  public SortedUnionPipe(List<? extends Pipe<T>> inputPipes, Comparator<? super T> comparator) {
    super(inputPipes, comparator);
  }

  /**
   * Constructor
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * IMPORTANT NOTE: comparator must be consistent with equals: a.equals(b) if and only if comparator.compare(a,b)==0.
   * @param inputPipes The input pipes. Each of them must be sorted by the order defined in the provided comparator, and items may have duplicates.
   */
  @SafeVarargs
  public SortedUnionPipe(Comparator<? super T> comparator, Pipe<T> ... inputPipes) {
    super(comparator, inputPipes);
  }

  @Override
  protected boolean shouldOutput(T item, BitSet pipesSeen) {
    return true; // All items should be sent to output, regardless of which input pipes they come from
  }

  @Override
  protected boolean canTerminate(BitSet activePipes) {
    return false; // An input pipe termination never means that this pipe should terminate 
  }

  @Override
  public float getProgress() {
    return PipeUtils.getMinProgress(getInputPipes());
  }
}  
