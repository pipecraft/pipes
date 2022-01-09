package org.pipecraft.pipes.sync.source;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.QueuePipeException;
import org.pipecraft.pipes.utils.QueueItem;

/**
 * A source pipe reading the contents of a {@link BlockingQueue}.
 *
 * This pipe is detached from the producer and has no knowledge on expected item count, therefore progress tracking is not possible
 * and simply jumps from 0.0 to 1.0 once the queue is fully consumed.
 * 
 * @param <T> the queue item data type
 *
 * @author Eyal Schneider
 */
public class QueueReaderPipe <T> implements Pipe<T> {
  private final BlockingQueue<QueueItem<T>> queue;
  private boolean complete;
  private T next;
  private PipeException exception;

  /**
   * Constructor
   *
   * @param queue The queue to read from
   */
  public QueueReaderPipe(BlockingQueue<QueueItem<T>> queue) {
    this.queue = queue;
  }

  /**
   * Constructor
   * 
   * @param queue The queue to read from
   * @param successMarker Used for indicating data completion with success. Should be a unique reference reserved for this purpose.
   * @param errorMarker Used for indicating an error termination. Should be a unique reference reserved for this purpose.
   * When found, the next() method will throw an exception.
   *
   * @deprecated Use the constructor without markers instead
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public QueueReaderPipe(BlockingQueue<T> queue, T successMarker, T errorMarker) {
    this.queue = (BlockingQueue<QueueItem<T>>) queue;
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    prepareNext();
  }

  @Override
  public float getProgress() {
    if (complete) {
      return 1.0f;
    }
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
  }

  // Throws QueuePipeException in case of a producer signaled error
  @Override
  public T next() throws PipeException, InterruptedException {
    if (exception != null) {
      throw exception;
    }
    T toReturn = next;
    prepareNext();
    return toReturn;
  }


  @Override
  public T peek() throws PipeException {
    if (exception != null) {
      throw exception;
    }
    return next;
  }

  public void prepareNext() throws InterruptedException {
    if (!complete) {
      QueueItem<T> queueItem = queue.take();
      if (queueItem.isSuccessfulEndOfData()) {
        complete = true;
        next = null;
      } else {
        Throwable e = queueItem.getThrowable();
        if (e != null) {
          complete = true;
          exception = new QueuePipeException("Error signaled by queue producer", e);
        } else {
          this.next = queueItem.getItem();
        }
      }
    }
  }

}
