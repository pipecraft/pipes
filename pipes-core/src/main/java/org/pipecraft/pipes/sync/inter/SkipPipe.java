package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe skipping the first K items of its input pipe. If the source pipe has m<=k items, the output of this pipe is empty.
 * 
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class SkipPipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private final int toSkip;
  
  /**
   * Constructor
   * @param input The input pipe
   * @param n The number of items to skip from the input pipe. 
   */
  public SkipPipe(Pipe<T> input, int n) {
    this.input = input;
    this.toSkip = n;
  }
  
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    return input.next();
  }

  @Override
  public T peek() throws PipeException {
    return input.peek();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    int i = 0;
    while (i++ < toSkip && input.next() != null);
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
