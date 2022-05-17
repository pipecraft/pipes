package org.pipecraft.pipes.sync.inter;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.QueueReaderPipe;
import org.pipecraft.pipes.terminal.QueueWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import org.pipecraft.pipes.utils.PipeUtils;
import org.pipecraft.pipes.utils.QueueItem;

/**
 * An intermediate pipe that actively pulls items from the input pipe and stores in a queue for the downstream pipe to fetch.
 * The threading model is a little different from other synchronous pipes: all upstream pipes are handled by an internal thread (the "produced thread" feeding the queue),
 * while all actions on this pipe are handled by the caller's thread. Therefore, positioning a QueuePipe in a flow will break the flow into producer part and consumer part,
 * each of them handled by a different thread.
 *
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class QueuePipe<T> extends CompoundPipe<T> {
  private final Pipe<T> inputPipe;
  private final BlockingQueue<QueueItem<T>> queue;

  /**
   * Constructor
   *
   * @param inputPipe The input pipe to pull items from
   * @param queue The queue to store data available for pulling from this pipe. Be careful with unbounded queues.
   */
  public QueuePipe(Pipe<T> inputPipe, BlockingQueue<QueueItem<T>> queue) {
    this.inputPipe = inputPipe;
    this.queue = queue;
  }

  /**
   * Constructor
   *
   * @param inputPipe The input pipe to pull items from
   * @param queueCapacity The capacity to use for the internal queue. Uses a {@link java.util.concurrent.LinkedBlockingQueue}.
   */
  public QueuePipe(Pipe<T> inputPipe, int queueCapacity) {
    this(inputPipe, new LinkedBlockingQueue<>(queueCapacity));
  }

  @Override
  protected Pipe<T> createPipeline() throws PipeException, InterruptedException {
    TerminalPipe qWriter = new QueueWriterPipe<>(inputPipe, queue);
    Pipe<T> qReader = new QueueReaderPipe<>(queue) {
      private Thread producerThread;

      public void close() throws IOException {
        super.close();
        producerThread.interrupt();
      }

      public void start() throws PipeException, InterruptedException {
        // start a thread blocking on queue writer start() method
        // Allow interruption by the consumer thread in case of close(). The producer thread Should also invoke close in this case.
        producerThread = new Thread(() -> {
          try {
            qWriter.start(); // Starts the producer side
            queue.put(QueueItem.end());
          } catch (PipeException | InterruptedException e) {
            // Interrupted exception will have the desired effect - the thread will terminate immediately.
            // A PipeException is already reported in the queue itself for the consumer to get notified.
          } finally {
            PipeUtils.close(qWriter);
          }
        });
        producerThread.start();
        super.start(); // Starts the consumer side
      }
    };
    return qReader;
  }
}
