package org.pipecraft.pipes.sync.source;

import java.io.IOException;
import java.util.function.Function;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe based on a given sequence generator function.
 * 
 * @author Eyal Schneider
 */
public class SeqGenPipe<T> implements Pipe<T> {
  private final Function<Long, T> generator;
  private final long totalCount;
  private volatile long pos;
  private T next;

  /**
   * Constructor
   * 
   * @param generator The function generating the i-th item produced by this pipe. Input starts from 0. 
   * Output of null means end of pipe. If g(i)=null, than for all j>i it's required that g(j)=null as well.
   */
  public SeqGenPipe(Function<Long, T> generator) {
    this (generator, Long.MAX_VALUE);
  }

  /**
   * Constructor
   * 
   * @param generator The function generating the i-th item produced by this pipe. Input starts from 0. 
   * @param count The required number of items to be produced by the pipe. Afterwards, null is constantly returned when calling next().
   * Note that if the generator function starts returning nulls earlier, then the pipe will produce less items than the supplied count.
   */
  public SeqGenPipe(Function<Long, T> generator, long count) {
    this.generator = generator;
    this.totalCount = count;
  }
  
  @Override
  public void close() throws IOException {
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  private void prepareNext() {
    if (pos < totalCount) {
      next = generator.apply(pos++);  
    } else {
      next = null;
    }
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    prepareNext();
  }

  @Override
  public float getProgress() {
    if (totalCount == 0) {
      return 1.0f;
    }
    if (totalCount == Long.MAX_VALUE) {
      return 0.0f;
    }
    
    return pos / (float)totalCount;
  }
}
