package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.pipecraft.pipes.utils.QueueItem;

/**
 * A terminal pipe writing items to a {@link BlockingQueue}.
 *
 * Note that this pipe can cause a deadlock if not used properly. Calling start() will start pushing to the queue in a blocking manner,
 * meaning that the consumer should be a different thread (or alternatively the queue should not be bounded, which is memory risky).
 *
 * @param <T> the queue item data type
 *
 * @author Eyal Schneider
 */
public class QueueWriterPipe<T> extends TerminalPipe {
  private final BlockingQueue<QueueItem<T>> queue;
  private final Pipe<T> inputPipe;

  /**
   * Constructor
   *
   * @param inputPipe     The pipe to read items from and push them to the queue
   * @param queue         The queue to write to
   *
   */
  @SuppressWarnings("unchecked")
  public QueueWriterPipe(Pipe<T> inputPipe, BlockingQueue<QueueItem<T>> queue) {
    this.inputPipe = inputPipe;
    this.queue = queue;
  }

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
   *
   * @deprecated Use the constructor without the queue markers instead
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public QueueWriterPipe(Pipe<T> inputPipe, BlockingQueue<T> queue, T successMarker, T errorMarker) {
    this.inputPipe = inputPipe;
    this.queue = (BlockingQueue<QueueItem<T>>) queue;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      inputPipe.start();
      T item;
      while ((item = inputPipe.next()) != null) {
        queue.put(QueueItem.of(item));
      }
      queue.put(QueueItem.end());
    } catch (Throwable e) {
      // Make an effort to release the consumer, regardless of the error type
      while (!queue.offer(QueueItem.error(e))) {
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