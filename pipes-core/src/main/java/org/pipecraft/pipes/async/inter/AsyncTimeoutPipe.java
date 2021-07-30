package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.TimeoutPipeException;

/**
 * An async pipe enforcing a timeout on completion of its async input pipe.
 * The timeout is reported as a {@link TimeoutPipeException} being reported in the notifyError(e) method.
 * 
 * @param <T> The item data type
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncTimeoutPipe <T> extends AsyncPipe<T> {
  private final AsyncPipe<T> input;
  private final long timeoutMillis;
  private final ScheduledExecutorService schedExecutor;
  private final AtomicBoolean isDone = new AtomicBoolean();
  private volatile ScheduledFuture<?> future;
  
  /**
   * constructor
   *
   * @param input The input pipe
   * @param timeout The time given to the input pipe to terminate, starting from the moment the input pipe is started
   * @param schedExector The scheduled executor used for checkout timeout
   */
  public AsyncTimeoutPipe(AsyncPipe<T> input, Duration timeout, ScheduledExecutorService schedExector) {
    this.input = input;
    this.timeoutMillis = timeout.toMillis();
    this.schedExecutor = schedExector;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    // Schedule timeout task
    future = schedExecutor.schedule(() -> {
      if (isDone.compareAndSet(false, true)) {
        try {
          notifyError(new TimeoutPipeException("Pipeline timeout - " + timeoutMillis + " millis ellapsed"));
          try {
            input.close();
          } catch (IOException e) {
            // Ignore
          }
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
    }, timeoutMillis, TimeUnit.MILLISECONDS);

    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(T item) throws PipeException, InterruptedException {
        notifyNext(item);
      }

      @Override
      public void done() throws InterruptedException {
        if (isDone.compareAndSet(false, true)) {
          notifyDone();
        }
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        if (isDone.compareAndSet(false, true)) {
          notifyError(e);
        }
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
    future.cancel(true);
    input.close();
  }
}
