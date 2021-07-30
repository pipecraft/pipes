package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.HeadPipe;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * An async version of the head pipe (See {@link HeadPipe}.
 * 
 * It guarantees that K items or less (in case there are less than K available by the input pipe) are produced.
 * Since async pipes in general don't guarantee any order, the identity of the produced items may not be consistent.
 * 
 * @param <T> The item data type
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncHeadPipe <T> extends AsyncPipe<T> {
  private final AsyncPipe<T> input;
  private final AtomicInteger remainingToRead;
  private final AtomicInteger completed;
  private final int totalToRead;
  private volatile boolean done;
  
  /**
   * constructor
   *
   * @param input The input pipe
   * @param count The number of items to pass on
   */
  public AsyncHeadPipe(AsyncPipe<T> input, int count) {
    this.input = input;
    this.totalToRead = count;
    this.remainingToRead = new AtomicInteger(count);
    this.completed = new AtomicInteger();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(T item) throws PipeException, InterruptedException {
        if (remainingToRead.get() <= 0) {
          return;
        }
        
        if (remainingToRead.getAndDecrement() > 0) {
          notifyNext(item);
          if (completed.incrementAndGet() == totalToRead) { // Establishes a happen-before relationship between notifyNext() and notifyDone(), as required
            new Thread(() -> { // An optimization to stop the input pipe from working, because we ignore the rest of the inputs anyway
                               // Must be done from another thread because close is blocking and may depend on the current method to terminate before exiting.
              PipeUtils.close(input);
            }).start();

            notifyDone();
          }
        } 
      }

      @Override
      public void done() throws InterruptedException {
        notifyDone();
        done = true;
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }});
    input.start();
  }

  @Override
  public float getProgress() {
    if (done) {
      return 1.0f;
    }
    return 1 - remainingToRead.get() / (float)totalToRead;
  }

  @Override
  public void close() throws IOException {
    super.close();
    input.close();
  }
}
