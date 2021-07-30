package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.commons.lang3.mutable.MutableInt;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.ProgressPipe;

/**
 * A no-op async pipe that reports progress.
 * Similar to {@link ProgressPipe}, but for async pipes.
 * Due to the fact that in the async version is more complex to track,
 * locking is used to guarantee monotonous progress reporting. This means
 * that it is recommended not to use small sample rates, and the progress reporting callback
 * should be as light as possible.
 * 
 * @author Eyal Schneider
 */
public class AsyncProgressPipe<T> extends AsyncPipe<T> {
  private final AsyncPipe<T> inputPipe;
  private final Consumer<Integer> callback;
  private final ThreadLocal<MutableInt> counter;
  private final int sampleRate;
  private final Object lock = new Object();
  private volatile int progressPct;

  /**
   * Constructor
   * 
   * @param inputPipe The upstream pipe
   * @param sampleRate For reducing computation overhead, The progress is being checked only every sampleRate items (approximately). 
   * @param callback The callback to be called with the new progress percentage when an increase is noticed.
   * The callback will be called only for unique integer percentage values, so it will be called at most 101 times (0-100).
   * The callback is invoked under a lock, so it doesn't have to use additional locking, unless it changes to state,
   * and some other caller thread is reading that state.
   * It's guaranteed that:
   * 1) The callback is first invoked with value 0
   * 2) The callback is invoked in strictly ascending progress order.
   * 3) Upon successful termination, the callback is invoked with value 100 
   */
  public AsyncProgressPipe(AsyncPipe<T> inputPipe,int sampleRate, Consumer<Integer> callback) {
    this.inputPipe = inputPipe;
    this.callback = callback;
    this.sampleRate = sampleRate;
    this.counter = ThreadLocal.withInitial(MutableInt::new);
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    inputPipe.setListener(new AsyncPipeListener<>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        if (counter.get().incrementAndGet() % sampleRate == 0) {
          update((int) (100 * inputPipe.getProgress()));  
        }
        notifyNext(item);
      }

      @Override
      public void done() throws InterruptedException {
        update(100);
        notifyDone();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }
    });
    callback.accept(0);
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
  
  private void update(int newProgressPct) {
    if (newProgressPct > progressPct) {
      synchronized (lock) { // Without locking we can't guarantee monotonicity
        if (newProgressPct > progressPct) {
          progressPct = newProgressPct; 
          callback.accept(newProgressPct);
        }
      }
    }
  }
}
