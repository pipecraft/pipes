package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.util.function.Predicate;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.FilterPipe;

/**
 * An async version of the filter pipe (See {@link FilterPipe}.
 * 
 * @param <T> The item data type
 * 
 * @author Eyal Schneider
 */
public class AsyncFilterPipe <T> extends AsyncPipe<T> {
  private final AsyncPipe<T> input;
  private final Predicate<? super T> predicate;

  /**
   * constructor
   *
   * @param input The input pipe
   * @param predicate The predicate defining which items to keep
   */
  public AsyncFilterPipe(AsyncPipe<T> input, Predicate<? super T> predicate) {
    this.input = input;
    this.predicate = predicate;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(T item) throws PipeException, InterruptedException {
        if (predicate.test(item)) {
          notifyNext(item);
        }
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
    input.close();
  }
}
