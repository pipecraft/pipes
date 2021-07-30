package org.pipecraft.pipes.async.source;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.EmptyPipe;

/**
 * An async source pipe returning no data.
 * Similar to {@link EmptyPipe}, but for async pipes.
 *
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class AsyncEmptyPipe<T> extends AsyncPipe<T> {

  @Override
  public void start() throws PipeException, InterruptedException {
    new Thread(() -> { // Running the done() callback in another thread, to fully comply with AsyncPipe's contract
      try {
        notifyDone();
      } catch (InterruptedException e) {
        // The thread stops anyway. Nothing to do here.
      }
    }).start();
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }
}
