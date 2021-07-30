package org.pipecraft.pipes.async;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * A dummy implementation of an async pipe for test purposes
 * 
 * @author Eyal Schneider
 */
public class DummyAsyncPipe extends AsyncPipe<Integer> {
  private final int start;
  private final int count;
  private final boolean endSuccessfully;
  private volatile boolean done;
  private volatile boolean externalCloseSignal;
  private volatile Thread startThread;

  public DummyAsyncPipe(int start, int count, boolean endSuccessfully) {
    this.start = start;
    this.count = count;
    this.endSuccessfully = endSuccessfully;
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    startThread = new Thread(() -> {
      try {
        ThreadPoolExecutor tp = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(500), new CallerRunsPolicy());
        tp.setRejectedExecutionHandler(new CallerRunsPolicy());
        
        for (int i = start; i < start + count; i++) {
          if (externalCloseSignal) {
            break;
          }
          final int j = i;
          tp.submit(() -> {
            try {
              if (!externalCloseSignal) {
                notifyNext(j);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
        }
        
        tp.shutdown(); // Allows submitted tasks to complete
        if (!tp.awaitTermination(2, TimeUnit.SECONDS)) {
          notifyError(new ValidationPipeException("Unable to terminate all tasks"));
        }
        done = true;
        
        if (!externalCloseSignal) { // If already closed from outside, don't produce additional items
          if (endSuccessfully) {
            notifyDone();            
          } else {
            notifyError(new ValidationPipeException("Test"));
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    startThread.start();
  }

  @Override
  public float getProgress() {
    return done ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    super.close();
    externalCloseSignal = true;
    try {
      startThread.join(); // To comply with requirement of no more notifications once this method exits
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
  }
}
