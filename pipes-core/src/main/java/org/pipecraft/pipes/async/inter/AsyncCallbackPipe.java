package org.pipecraft.pipes.async.inter;

import java.io.IOException;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.concurrent.FailableConsumer;
import org.pipecraft.pipes.sync.inter.CallbackPipe;

/**
 * A no-op async pipe that "spies" on data passing through it.
 * Similar to {@link CallbackPipe}, but for async pipes
 * 
 * @author Eyal Schneider
 */
public class AsyncCallbackPipe<T> extends AsyncPipe<T> {
  private final AsyncPipe<T> inputPipe;
  private final FailableConsumer<? super T, ? extends PipeException> callback;

  /**
   * Constructor
   * 
   * @param inputPipe The upstream pipe
   * @param itemCallback The callback to be notified on every item passing through this pipe
   */
  public AsyncCallbackPipe(AsyncPipe<T> inputPipe, FailableConsumer<? super T, ? extends PipeException> itemCallback) {
    this.inputPipe = inputPipe;
    this.callback = itemCallback;
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    inputPipe.setListener(new AsyncPipeListener<>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        callback.accept(item);
        notifyNext(item);
      }

      @Override
      public void done() throws InterruptedException {
        notifyDone();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }
    });
    inputPipe.start();
  }

  @Override
  public float getProgress() {
    return inputPipe.getProgress();
  }

  @Override
  public void close() throws IOException {
    super.close();
    inputPipe.close();
  }
}
