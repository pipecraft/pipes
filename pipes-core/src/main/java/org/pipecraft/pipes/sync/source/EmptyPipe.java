package org.pipecraft.pipes.sync.source;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe with no data in it.
 * 
 * @param <T> The pipe's data type
 *
 * @author Eyal Schneider
 */
public class EmptyPipe<T> implements Pipe<T> {
  private static final EmptyPipe<?> INSTANCE = new EmptyPipe<>();

  /**
   * Private constructor
   */
  private EmptyPipe() {
  }
  
  /**
   * @return The empty pipe instance. Can be safely shared and re-used.
   */
  @SuppressWarnings("unchecked")
  public static <C extends Pipe<?>> C instance() {
    return (C) INSTANCE;
  }
  
  @Override
  public void close() throws IOException {
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    return null;
  }

  @Override
  public T peek() {
    return null;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }
}
