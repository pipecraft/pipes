package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.pipecraft.pipes.BasePipe;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;
import org.pipecraft.pipes.utils.PipeUtils;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe that consumes multiple input pipes in a parallel manner, with a configurable number of threads.
 * For thread safety reasons, it is guaranteed that each pipe is consumed by exactly one thread.
 * 
 * Each of the input pipes may be of any type: sync, async or terminal.
 * 
 * @author Eyal Schneider
 */
public class ParallelConsumerPipe extends TerminalPipe {
  private final Collection<? extends BasePipe> inputs;
  private final int parallelism;
  private final Runnable terminationAction;

  /**
   * Constructor
   * 
   * @param parallelism The number of threads to use for consuming all pipes
   * @param inputs The input pipes
   * @param terminationAction An action to perform once all input items from all pipes have been consumed. Runs once, upon a successful iteration termination only.
   * Must be thread safe!
   */
  public ParallelConsumerPipe(int parallelism, Collection<? extends BasePipe> inputs, Runnable terminationAction) {
    this.parallelism = parallelism;
    this.inputs = inputs;
    this.terminationAction = terminationAction;
  }

  /**
   * Constructor
   * 
   * @param parallelism The number of threads to use for consuming all pipes
   * @param inputs The input pipes
   */
  public ParallelConsumerPipe(int parallelism, Collection<? extends BasePipe> inputs) {
    this(parallelism, inputs, () -> {});
  }

  /**
   * Constructor
   * 
   * @param parallelism The number of threads to use for consuming all pipes
   * @param inputs The input pipes
   */
  public ParallelConsumerPipe(int parallelism, BasePipe ... inputs) {
    this(parallelism, Arrays.asList(inputs));
  }
  
  @Override
  public void close() throws IOException {
    // No need to close anything here, since start() method makes sure to close any started input pipe
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    ParallelTaskProcessor.runFailable(inputs, parallelism, p -> {
      Exception pe = null;
      try {
        if (p instanceof Pipe<?>) { // Sync pipe
          consumeSyncPipe((Pipe<?>)p);
        } else if (p instanceof AsyncPipe<?>) { // Async pipe
          consumeAsyncPipe((AsyncPipe<?>)p);
        } else if (p instanceof TerminalPipe) { // Terminal pipe
          consumeTerminalPipe((TerminalPipe)p); 
        } else {
          throw new IllegalArgumentException("Unsupported pipe type: " + p.getClass().getSimpleName());
        }
      } catch (InterruptedException e) {
       Thread.currentThread().interrupt(); // Interruption here means that some other ParallelTaskProcessor task failed, so we should exit immediately 
      } catch (PipeException | RuntimeException e) {
        pe = e;
        throw e;
      } finally {
        try {
          p.close();
        } catch (IOException e) {
          PipeException toThrow = new IOPipeException(e);
          if (pe != null) {
            toThrow.addSuppressed(pe);
          }
          throw toThrow;
        }
      }
    });
    terminationAction.run();
  }

  @Override
  public float getProgress() {
    return PipeUtils.getAverageProgress(inputs);
  }
  
  private static void consumeAsyncPipe(AsyncPipe<?> asyncPipe) throws InterruptedException, PipeException {
    CountDownLatch terminationLatch = new CountDownLatch(1);
    AtomicReference<PipeException> exception = new AtomicReference<>();
    asyncPipe.setListener(new AsyncPipeListener<Object>() {
      @Override
      public void next(Object item) throws PipeException, InterruptedException {
      }

      @Override
      public void done() throws InterruptedException {
        terminationLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        exception.set(e);
        terminationLatch.countDown();
      }
    });
    terminationLatch.await();
    if (exception.get() != null) {
      throw exception.get();
    }
  }

  private static void consumeSyncPipe(Pipe<?> p) throws PipeException,InterruptedException {
    p.start();
    while(p.next() != null);
  }
  
  private static void consumeTerminalPipe(TerminalPipe p) throws PipeException,InterruptedException {
    p.start();
  }
}
