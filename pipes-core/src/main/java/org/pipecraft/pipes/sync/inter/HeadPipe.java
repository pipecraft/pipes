package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe returning the first K items of its input pipe. If the source pipe has m<k items, the m items are returned.
 * 
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class HeadPipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private final int totalCount;
  private volatile int remainingCount;
  private T next;
  
  /**
   * Constructor
   * @param input The input pipe
   * @param n The number of items to return from the input pipe. 
   */
  public HeadPipe(Pipe<T> input, int n) {
    this.input = input;
    this.totalCount = n;
    this.remainingCount = n;
  }
  
  @Override
  public void close() throws IOException {
    input.close();
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

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    prepareNext();
  }

  private void prepareNext() throws PipeException, InterruptedException {
    if (remainingCount == 0) {
      next = null;
    } else {
      remainingCount--;
      next = input.next();
    }
  }

  @Override
  public float getProgress() {
    if (totalCount == 0) {
      return 1.0f;
    }
    return 1.0f - remainingCount / (float)totalCount;
  }
}
