package org.pipecraft.pipes.sync.inter;

import java.util.BitSet;
import java.util.Comparator;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.sync.Pipe;

/**
 * An intermediate pipe performing a set subtraction operation on two given pipes (i.e. returns items from pipe A which aren't found in pipe B).
 * 
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
public class SortedSubtractionPipe<T> extends MultiSortedBasePipe<T> {

  /**
   * Constructor
   * 
   * @param pipeA left side pipe (The pipe we are subtracting from) 
   * @param pipeB right side pipe
   * @param comparator The total order relation to use when merging the data from the different input pipes.
   * Should be consistent with the ordering of the input pipes. 
   * IMPORTANT NOTE: comparator must be consistent with equals: a.equals(b) if and only if comparator.compare(a,b)==0.
   */
  public SortedSubtractionPipe(Pipe<T> pipeA, Pipe<T> pipeB, Comparator<? super T> comparator) {
    super(Lists.newArrayList(pipeA, pipeB), comparator);
  }

  @Override
  protected boolean shouldOutput(T item, BitSet pipesSeen) {
    return pipesSeen.get(0) && !pipesSeen.get(1);
  }
  
  @Override
  protected boolean canTerminate(BitSet activePipes) {
    return !activePipes.get(0);
  }
  
  @Override
  public float getProgress() {
    return getInputPipes().get(0).getProgress();
  }
}  
