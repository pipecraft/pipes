package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;

/**
 * A base-class pipe implementation for pipes that filter an input pipe using some criteria.
 * 
 * @author Eyal Schneider
 */
public abstract class FilterBasePipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private T next;
  
  /**
   * Constructor
   *
   * @param input The input pipe
   */
  public FilterBasePipe(Pipe<T> input) {
    this.input = input;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    prepareNext();
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
  public float getProgress() {
    return input.getProgress();
  }

  /**
   * @param v The next item from the input stream
   * @return true if and only if the item should be selected
   */
  protected abstract boolean shouldSelect(T v);
  
  private void prepareNext() throws PipeException, InterruptedException {
    while ((next = input.next()) != null && !shouldSelect(next));
  }
}
