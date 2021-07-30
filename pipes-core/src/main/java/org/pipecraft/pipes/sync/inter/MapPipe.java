package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;

/**
 * A pipe transforming each item in the input pipe into another, possibly with a different type.
 * The transformation function is assumed to be error safe - i.e. this pipe isn't suitable if the transformation itself
 * may fail with a checked exceptions.
 *
 * @param <S> The source item data type
 * @param <T> The target item data type
 *
 * @author Eyal Schneider
 */
public class MapPipe<S, T> implements Pipe<T> {

  private final Pipe<S> input;
  private final FailableFunction<? super S, T, PipeException> mapFunction;
  private T next;

  /**
   * constructor
   *
   * @param input The input pipe
   * @param mapFunction The function to apply to each item
   */
  public MapPipe(Pipe<S> input, FailableFunction<? super S, T, PipeException> mapFunction) {
    this.input = input;
    this.mapFunction = mapFunction;
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

  private void prepareNext() throws PipeException, InterruptedException {
    S nextFromIn = input.next();
    if (nextFromIn == null) {
      next = null;
    } else {
      next = mapFunction.apply(nextFromIn);
    }
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
