package org.pipecraft.pipes.sync.source;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe with no data in it, which only produces a predefined {@link PipeException}.
 * Typically useful for tests
 * 
 * @param <T> The pipe's data type
 *
 * @author Eyal Schneider
 */
public class ErrorPipe<T> implements Pipe<T> {
  private final PipeException exception;

  /**
   * Constructor
   * 
   * @param exception The exception to throw upon calls to next()
   */
  public ErrorPipe(PipeException exception) {
    this.exception = exception;
  }
  
  @Override
  public void close() throws IOException {
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    throw exception;
  }

  @Override
  public T peek() throws PipeException {
    throw exception;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
  }

  @Override
  public float getProgress() {
    return 0.0f;
  }
}
