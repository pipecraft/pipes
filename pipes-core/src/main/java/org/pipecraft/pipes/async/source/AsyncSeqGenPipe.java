package org.pipecraft.pipes.async.source;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * An async source pipe, producing a fixed number of items using a fixed number of threads.
 * 
 * @param <T> The items' data type
 * 
 * @author Eyal Schneider
 */
public class AsyncSeqGenPipe <T> extends AsyncPipe<T>{
  private final long count;
  private final Function<Long, T> generator;
  private final int threadCount;
  private final AtomicLong producedCount = new AtomicLong();
  private volatile boolean closeRequested;
  private final CountDownLatch closeLatch = new CountDownLatch(1);
  
  /**
   * Constructor
   * 
   * @param count The number of items to generate
   * @param generator The function generating the i-th item produced by this pipe. Input range from 0 to count - 1, inclusive. 
   * Outputs must not be null. 
   * @param threadCount The number of threads producing the items
   */
  public AsyncSeqGenPipe(long count, Function<Long, T> generator, int threadCount) {
    this.count = count;
    this.generator = generator;
    this.threadCount = threadCount;
  }

  
  @Override
  public void start() throws PipeException, InterruptedException {
    
    new Thread(() -> {
      try {
        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
          final int start = t;
          Thread thread = new Thread(() -> {
            try {
              for (long i = start; i < count; i+= threadCount) {
                if (closeRequested) { // Exit the thread as soon as possible if close was requested
                  return;
                }
                T item = generator.apply(i);
                notifyNext(item);
                producedCount.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
          thread.start();
          threads[t] = thread;
        }
        
        for (Thread t : threads) {
          t.join();
        }
        
        notifyDone(); // Internally protects from actual done notification if close() has been called
      } catch (InterruptedException e) {
        // we exit the thread at this point, so no special handling is needed
      } finally {
        closeLatch.countDown(); // Release the thread waiting on the close() method, if any
      }
    }).start(); // We are not allowed to block the start method in an async pipe so we run this in the background
  }

  @Override
  public float getProgress() {
    return producedCount.get() / (float)count;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closeRequested = true;
    try {
      closeLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Re-set the interruption flag and exit
    }
  }
}
