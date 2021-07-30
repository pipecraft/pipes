package org.pipecraft.pipes.async.inter;

import java.io.IOException;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.sync.inter.MapPipe;

/**
 * An async version of the mapper pipe (See {@link MapPipe}.
 * 
 * @param <S> The source item data type
 * @param <T> The target item data type
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncMapPipe <S, T> extends AsyncPipe<T> {
  private final AsyncPipe<S> input;
  private final FailableFunction<? super S, T, PipeException> mapFunction;

  /**
   * constructor
   *
   * @param input The input pipe
   * @param mapFunction The function to apply to each item
   */
  public AsyncMapPipe(AsyncPipe<S> input, FailableFunction<? super S, T, PipeException> mapFunction) {
    this.input = input;
    this.mapFunction = mapFunction;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(S item) throws PipeException, InterruptedException {
        notifyNext(mapFunction.apply(item));
      }

      @Override
      public void done() throws InterruptedException {
        notifyDone();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }});
    input.start();
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }

  @Override
  public void close() throws IOException {
    super.close();
    input.close();
  }
}
