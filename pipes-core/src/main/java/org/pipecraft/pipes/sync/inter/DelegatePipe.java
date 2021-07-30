package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe delegating all work to another pipe given in the constructor.
 * 
 * @author Eyal Schneider
 */
public abstract class DelegatePipe<T> implements Pipe<T> {
  private final Pipe<T> originPipe;

  /**
   * Constructor
   * 
   * @param originPipe The pipe to delegate work to
   */
  public DelegatePipe(Pipe<T> originPipe) {
    this.originPipe = originPipe;
  }
  
  @Override
  public void close() throws IOException {
    originPipe.close();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    return originPipe.next();
  }

  @Override
  public T peek() throws PipeException {
    return originPipe.peek();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    originPipe.start();
  }

  @Override
  public float getProgress() {
    return originPipe.getProgress();
  }
  
  /**
   * @return The pipe we delegate work to
   */
  public Pipe<T> getOriginPipe() {
    return originPipe;
  }
}
