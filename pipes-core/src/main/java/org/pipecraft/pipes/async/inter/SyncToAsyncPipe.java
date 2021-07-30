package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * A pipe converting multiple sync pipes into a single async one.
 * The caller can configure the number of threads used for publishing the input pipes' items.
 * The implementation makes sure to maintain thread safety of the synchronous input pipes by handling each pipe using a single thread.
 *
 * @author Eyal Schneider
 */
public class SyncToAsyncPipe<T> extends AsyncPipe<T> {
  private final Collection<PipeSupplier<T>> inputPipeSuppliers;
  private final int threadCount;
  private final AtomicInteger donePipesCount;
  private Thread startThread;
  
  /**
   * Constructor
   *
   * @param inputPipeSuppliers  The input pipe suppliers (allowing lazy initialization). The produced pipes are not required to be thread safe.
   * @param threadCount The number of threads to actively fetch data from the input pipes.
   *                    Thread compete on "acquiring" pipes and then draining them. There's no benefit
   *                    in setting number of threads larger than the number of pipes.
   */
  public SyncToAsyncPipe(Collection<PipeSupplier<T>> inputPipeSuppliers, int threadCount) {
    this.inputPipeSuppliers = inputPipeSuppliers;
    this.threadCount = threadCount;
    this.donePipesCount = new AtomicInteger(0);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    startThread = new Thread( () -> {
      boolean error = false;
      try {
        ParallelTaskProcessor.runFailable(inputPipeSuppliers, threadCount, this::drainPipe);
      } catch (PipeException e) {
        error = true;
        notifyErrorExitOrInterrupt(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        if (!error) { // We make sure: 1) To notify done only once 2) To not notify both error and done events
          try {
            notifyDone();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Mark thread as interrupted and exit
          }
        }
      }
    });
    
    startThread.start(); // We don't want start() to block, as required by the async pipes contract
  }

  /**
   * @return Percent of pipes that were finished.
   */
  @Override
  public float getProgress() {
    return donePipesCount.floatValue() / inputPipeSuppliers.size();
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (startThread == null) {
      // start never called/finished
      return;
    }

    // Stop the start-thread before exiting, to guarantee that there's no more work on any pipe
    startThread.interrupt();
    try {
      startThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while waiting for threads to terminate");
    }
  }

  /**
   * @param inputs A collection of pipes to be fed into the {@link SyncToAsyncPipe}
   * @param threadCount The number of threads to actively fetch data from the input pipes. Must not exceed the number of input pipes.
   *                    Each thread is assigned an exclusive set of pipes to read from.
   * @return A new {@link SyncToAsyncPipe} initialized with the given input pipes
   */
  public static <T> SyncToAsyncPipe<T> fromPipes(Collection<Pipe<T>> inputs, int threadCount) {
    return new SyncToAsyncPipe<>(inputs.stream().map(p -> (PipeSupplier<T>)(() -> p)).collect(Collectors.toList()), threadCount);
  }

  /**
   * @param input The pipe to be fed into the {@link SyncToAsyncPipe}
   * @return A new {@link SyncToAsyncPipe} initialized with the given input pipes
   */
  public static <T> SyncToAsyncPipe<T> fromPipe(Pipe<T> input) {
    return new SyncToAsyncPipe<>(Collections.singleton(() -> input), 1);
  }

  private void notifyErrorExitOrInterrupt(PipeException e) {
    try {
      notifyError(e);
    } catch (InterruptedException e2) {
      Thread.currentThread().interrupt(); // Re-mark thread as interrupted
    }
  }

  private void drainPipe(PipeSupplier<T> pipeSupplier) throws PipeException {
    Pipe<T> pipe = null;
    try {
      pipe = pipeSupplier.get();
      pipe.start();
      T next;
      while ((next = pipe.next()) != null && !Thread.currentThread().isInterrupted()) {
        notifyNext(next);
      }

      pipe.close();
      donePipesCount.incrementAndGet();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Mark thread as interrupted and exit
    } catch (IOException e) {
      throw new IOPipeException(e);
    } finally {
      PipeUtils.close(pipe);
    }
  }
}
