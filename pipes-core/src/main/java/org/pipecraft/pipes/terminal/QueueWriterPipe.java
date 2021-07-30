package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * A terminal pipe writing items to a {@link BlockingQueue}.
 * Uses special item values as queue end marker indicators.
 * For proper behavior, the same marker references (one for error and one for successful termination) should also
 * be used by the queue consumer.
 *
 * Note that this pipe can cause a deadlock if not used properly. Calling start() will start pushing to the queue in a blocking manner,
 * meaning that the consumer should be a different thread (or alternatively the queue should not be bounded, which is memory risky).
 *
 * @param <T> the queue item data type
 *
 * @author Eyal Schneider
 */
public class QueueWriterPipe<T> extends TerminalPipe {

  private final BlockingQueue<T> queue;
  private final T successMarker;
  private final T errorMarker;
  private final Pipe<T> inputPipe;

  /**
   * Constructor
   *
   * @param inputPipe     The pipe to read items from and push them to the queue
   * @param queue         The queue to write to
   * @param successMarker Used for indicating data completion with success. Should be a unique
   *                      reference reserved for this purpose. Consumer should use the same marker
   *                      reference for end of input indication.
   * @param errorMarker   Used for indicating an error termination. Should be a unique reference
   *                      reserved for this purpose. Consumer should use the same marker reference
   *                      for error indication.
   */
  public QueueWriterPipe(Pipe<T> inputPipe, BlockingQueue<T> queue, T successMarker,
      T errorMarker) {
    this.inputPipe = inputPipe;
    this.queue = queue;
    this.successMarker = successMarker;
    this.errorMarker = errorMarker;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      inputPipe.start();
      T item;
      while ((item = inputPipe.next()) != null) {
        queue.put(item);
      }
      queue.put(successMarker);
    } catch (Throwable e) {
      // Make an effort to release the consumer, regardless of the error type
      while (!queue.offer(errorMarker)) {
        queue.clear();
      }
      throw e;
    }
  }

  @Override
  public float getProgress() {
    return inputPipe.getProgress();
  }

  @Override
  public void close() throws IOException {
    inputPipe.close();
  }
}