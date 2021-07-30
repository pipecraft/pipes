package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.utils.PipeFactory;

/**
 * An intermediate pipe that maps an input item into zero or more items of another type.
 * This is a more flexible version of the {@link MapPipe}, which maps one to one.   
 * 
 * @author Eyal Schneider
 *
 */
public class FlexibleMapPipe <S, T> implements Pipe<T> {
  private final Pipe<S> input;
  private final PipeFactory<S, ? extends T> factory;
  private Pipe<? extends T> currPipe = EmptyPipe.instance();
  private T next;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param factory A factory mapping an input item to a pipe of items of the target type
   */
  public FlexibleMapPipe(Pipe<S> input, PipeFactory<S, ? extends T> factory) {
    this.input = input;
    this.factory = factory;
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
    try {
      while ((next = currPipe.next()) == null) {
        currPipe.close();
        S nextFromInput = input.next();
        if (nextFromInput == null) {
          break;
        }
        currPipe = factory.get(nextFromInput);
        currPipe.start();
      }
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
