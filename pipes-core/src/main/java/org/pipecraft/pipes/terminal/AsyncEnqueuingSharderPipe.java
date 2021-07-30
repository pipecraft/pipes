package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.infra.math.ArithmeticUtils;
import org.pipecraft.pipes.exceptions.InternalPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe that receives an async pipe as input, and shards the contents of the input pipe into multiple queues
 * according to some sharding criteria based on item values.
 * In case of relatively few shards this option is a good alternative to {@link AsyncSharderPipe} (when used as an intermediate step),
 * because it doesn't involve disk IO.
 * 
 * In case of errors, the start() method unblocks and exits with the exception, as required by the spec.
 * In addition, queue consumers will read a special error marker placed by this class.
 * Similarly, completion is reported by sending a completion marker to all queues.
 * 
 * The implementation allows calling close() by any thread after start() has been invoked.
 * In case of a premature close, no markers (error/success) are sent to the output queues,
 * meaning that the caller is responsible for releasing them.
 * 
 * Caveats:
 * 
 * 1. This implementation fills multiple queues, so it's recommended to use bounded queues and be aware of their total memory consumption. 
 * 2. In order to prevent a deadlock, the caller should make sure to not start queue consumers
 * and the start() method by the same thread. Alternatively, one can use the asyncStart() method.
 * 3. Queue consumers should not try to drain some queues before others using blocking calls, since it will result in a deadlock.
 * 4. Queue consumers should be aware of the reserved error and successful completion markers, and handle them differently than a standard item.
 * 
 * @param <T> The items data type
 *
 * @author Eyal Schneider
 */
public class AsyncEnqueuingSharderPipe<T> extends TerminalPipe {
  private final AsyncPipe<T> input;
  private final Function<? super T, Integer> selector;
  private final List<? extends BlockingQueue<T>> queues;
  private final AtomicInteger[] updatingCounts;
  private volatile int[] finalCounts;
  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private volatile PipeException error;
  private volatile boolean closed;
  private final T successMarker;
  private final T errorMarker;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param queues The queues to write to. The order indicates their identities used by the selector function.
   * @param selectorFunction Given an item, selects the index of the queue to write the item to.
   * Must return an integer between 0 and queues.size() - 1.
   * @param successMarker A special (reserved reference) item value used for indicating a successful completion to queue consumers
   * @param errorMarker A special (reserved reference) item value used for indicating an error to queue consumers.
   */
  public AsyncEnqueuingSharderPipe(AsyncPipe<T> input, List<? extends BlockingQueue<T>> queues, Function<? super T, Integer> selectorFunction, T successMarker, T errorMarker) {
    this.input = input;
    this.queues = new ArrayList<>(queues);
    this.updatingCounts = new AtomicInteger[queues.size()];
    for (int i = 0; i < updatingCounts.length; i++) {
      updatingCounts[i] = new AtomicInteger();
    }
    this.selector = selectorFunction;
    this.successMarker = successMarker;
    this.errorMarker = errorMarker;
  }

  /**
   * Constructor
   * 
   * Uses hash based sharding into queues
   * @param input The input pipe
   * @param queues The queues to write to. The order indicates their identities used by the selector function.
   * @param successMarker A special (reserved reference) item value used for indicating a successful completion to queue consumers
   * @param errorMarker A special (reserved reference) item value used for indicating an error to queue consumers.
   */
  public AsyncEnqueuingSharderPipe(AsyncPipe<T> input, List<? extends BlockingQueue<T>> queues, T successMarker, T errorMarker) {
    this(input, queues, v -> ArithmeticUtils.getShardByHash(v, queues.size()), successMarker, errorMarker);
  }

  @Override
  public void close() throws IOException {
    input.close();
    closed = true;
    terminationLatch.countDown(); // Release caller thread running the start() method
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        int index = selector.apply(item);
        updatingCounts[index].incrementAndGet();
        queues.get(index).put(item);
      }

      @Override
      public void done() throws InterruptedException {
        terminationLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        error = e;
        terminationLatch.countDown();
      }
    });
    
    input.start();
    terminationLatch.await(); // Block until all items are processed, an error occurs or close() is called
    
    // Place termination markers (errors/success) in all queues
    if (!closed) {
      T marker = error == null ? successMarker : errorMarker;
      for (BlockingQueue<T> q : queues) {
        q.put(marker); //TODO(EyalS): consider adding timeout here, because otherwise the total blocking time depends on the consumers
      }
    }

    // Throw error, if any
    if (error != null) {
      if (error instanceof InternalPipeException) {
        throw ((InternalPipeException) error).getRuntimeException(); // Transforming the wrapper to the actual runtime exception it should be
      }
      throw error;
    }
        
    // Publish stats (using a safe volatile write)
    int[] counts = new int[queues.size()];
    for (int i = 0; i < counts.length; i++) {
      counts[i] = updatingCounts[i].get();
    }
    this.finalCounts = counts;
  }

  /**
   * A special async version of the standard start() method.
   * The caller may run this method before starting the queue consumers or vice versa, without
   * risking with deadlock. The returned future can be used to detect pipe completion, 
   * and to get the exception, if any.
   * 
   * @return the future representing the completion of this terminal pipe
   */
  public Future<Void> asyncStart() {
    ExecutorService ex = Executors.newSingleThreadExecutor();
    Future<Void> f = ex.submit(() -> { start(); return null; });
    ex.shutdown(); // Initiates a shutdown without interrupting the job
    return f;
  }
  
  /**
   * @return The counts of items written to each shard, as an array. 
   * Item i corresponds to queue #i. 
   * Call this method only after start() has been called and completed successfully. 
   */
  public int[] getShardSizes() {
    return finalCounts;
  }
  
  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
