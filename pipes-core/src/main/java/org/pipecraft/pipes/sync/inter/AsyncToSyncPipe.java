package org.pipecraft.pipes.sync.inter;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import org.pipecraft.pipes.exceptions.QueuePipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.utils.QueueItem;

/**
 * A pipe which acts as a converter from async pipe/s to a sync pipe.
 * Uses a blocking queue for collecting items and supplying them to the downstream synchronous consumer.
 * 
 * @author Eyal Schneider
 */
public class AsyncToSyncPipe<T> implements Pipe<T> {
  private final AsyncPipe<T> inputPipe;
  private final BlockingQueue<QueueItem<T>> queue;
  private boolean done;

  /**
   * Constructor
   *
   * @param inputPipe The single input async pipe
   * @param queue The blocking queue to use for storing the items produced by the input pipes and supplying them. May be bounded/unbounded.
   * Use unbounded queues with caution.
   */
  public AsyncToSyncPipe(AsyncPipe<T> inputPipe, BlockingQueue<QueueItem<T>> queue) {
    this.inputPipe = inputPipe;
    this.queue = queue;
  }

  /**
   * Constructor
   *
   * Uses a LinkedBlockingQueue with the given capacity
   *
   * @param inputPipe The single input async pipe
   * @param queueCapacity The queue capacity. When reached, the threads of the input pipes get blocked when trying to enqueue items.
   */
  public AsyncToSyncPipe(AsyncPipe<T> inputPipe, int queueCapacity) {
    this(inputPipe, new LinkedBlockingQueue<>(queueCapacity));
  }

  /**
   * Constructor
   * 
   * @param inputPipe The single input async pipe
   * @param queue The blocking queue to use for storing the items produced by the input pipes and supplying them. May be bounded/unbounded.
   * Use unbounded queues with caution.
   * @param markerFactory A generator of special instances of the item data type, for internal uses. Should generate new instances, and the instances should not be used by the caller.
   * @deprecated Use the constructor with no markerFactory instead
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public AsyncToSyncPipe(AsyncPipe<T> inputPipe, BlockingQueue<T> queue, Supplier<T> markerFactory) {
    this.inputPipe = inputPipe;
    this.queue = (BlockingQueue<QueueItem<T>>) queue;
  }

  /**
   * Constructor
   * 
   * Uses a LinkedBlockingQueue with the given capacity
   * 
   * @param inputPipe The single input async pipe
   * @param queueCapacity The queue capacity. When reached, the threads of the input pipes get blocked when trying to enqueue items.
   * @param markerFactory A generator of special instances of the item data type, for internal uses. Should generate new instances, and the instances should not be used by the caller.
   * @deprecated Use the constructor with no markerFactory instead
   */
  @Deprecated
  public AsyncToSyncPipe(AsyncPipe<T> inputPipe, int queueCapacity, Supplier<T> markerFactory) {
    this(inputPipe, new LinkedBlockingQueue<>(queueCapacity), markerFactory);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    Listener l = new Listener();
    inputPipe.setListener(l);
    inputPipe.start();
  }

  @Override
  public float getProgress() {
    return inputPipe.getProgress();
  }

  @Override
  public void close() throws IOException {
    inputPipe.close();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    if (done) {
      return null;
    }
    
    QueueItem<T> itemW = queue.take();
    if (itemW.isSuccessfulEndOfData()) {
      done = true;
      return null;
    }
    Throwable e = itemW.getThrowable();
    if (e != null) {
      done = true;
      throw new QueuePipeException("Error signaled by queue producer", e);
    }
    return itemW.getItem();
  }

  @Override
  public T peek() throws PipeException {
    try {
      QueueItem<T> itemW;
      while ((itemW = queue.peek()) == null) {
        Thread.sleep(10); // We have no choice but to block here. A returned value of null would be incorrect because it means end of data in Pipes.
      }
      if (itemW.isSuccessfulEndOfData()) {
        return null;
      }
      Throwable e = itemW.getThrowable();
      if (e != null) {
        throw new QueuePipeException("Error signaled by queue producer", e);
      }
      return itemW.getItem();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // The peek() API doesn't allow propagating the exception. We exit immediately and mark the thread as interrupted instead.
      return null;
    }
  }

  private class Listener implements AsyncPipeListener<T> {

    @Override
    public void next(T item) throws PipeException, InterruptedException {
      queue.put(QueueItem.of(item));
    }

    @Override
    public void done() throws InterruptedException {
      queue.put(QueueItem.end());
    }

    @Override
    public void error(PipeException e) throws InterruptedException {
      queue.put(QueueItem.error(e));
    }
    
  }
}
