package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe encapsulating a pipeline.
 * Subclasses should implement createPipeline() method.
 * 
 * @author Eyal Schneider
 */
public abstract class CompoundPipe<T> implements Pipe<T> {
  private Pipe<T> innerPipe;
  
  @Override
  public void close() throws IOException {
    if (innerPipe != null) {
      innerPipe.close();
    }
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    return innerPipe.next();
  }

  @Override
  public T peek() throws PipeException {
    return innerPipe.peek();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    innerPipe = createPipeline();
    innerPipe.start();
  }

  /**
   * @return A new pipeline to represent the logic of this pipe
   * @throws PipeException In case of a pipeline creation error
   * @throws InterruptedException In case that the thread is interrupted
   */
  protected abstract Pipe<T> createPipeline() throws PipeException, InterruptedException;

  @Override
  public float getProgress() {
    return innerPipe.getProgress();
  }
}
