package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * A no-op pipe with only one side effect - progress tracking.
 * Progress in the input pipe is tracked by a callback method being called each time there's a progress in at least 1%.
 * 
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class ProgressPipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private final Consumer<Integer> callback;
  private final int sampleRate;
  private int countSinceLastSample;
  private T next;
  private int progressPct;
  
  /**
   * Constructor
   * @param input The input pipe
   * @param sampleRate For reducing computation overhead, The progress is being checked only every sampleRate items. 
   * @param callback The callback to be called with the new progress percentage when an increase is noticed.
   * The callback will be called only for unique integer percentage values, so it will be called at most 101 times (0-100).
   */
  public ProgressPipe(Pipe<T> input, int sampleRate, Consumer<Integer> callback) {
    this.input = input;
    this.sampleRate = sampleRate;
    this.callback = callback;
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
    callback.accept(0);
    prepareNext();
  }

  private void prepareNext() throws PipeException, InterruptedException {
    next = input.next();
    if (next == null) {
      update(100);
    } else if (++countSinceLastSample == sampleRate) {
      update((int) (100 * input.getProgress()));  
      countSinceLastSample = 0;
    }
  }

  private void update(int newProgressPct) {
    if (newProgressPct > progressPct) {
      progressPct = newProgressPct;
      callback.accept(newProgressPct);
    }
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
