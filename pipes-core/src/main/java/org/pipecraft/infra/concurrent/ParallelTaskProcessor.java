package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * Processes a heavy task (typically with IO involved) in parallel. 
 * The input is a set of task items that have no inter-dependencies and therefore can be processed in parallel.
 * The caller supplies both the item processor and the items to work on.
 * 
 * @author Eyal Schneider
 */
public class ParallelTaskProcessor {

  /**
   * Processed the items in parallel, by running a given processor on them, using the provided executor.
   * The processor here is allowed to throw checked exceptions, of type E.
   * 
   * Notes:
   * 1) In case of an error while processing one of the items, the method tries to exit as soon as possible. 
   * 2) The method returns only after making sure there aren't any pending items.
   * 3) It follows from (1) and (2) that the processor provided here should preferably be interruptible. This way we can exit faster when an error occurs in any of the items.
   * 
   * @param ex The executor to use for running the processor on. It is recommended that the number of background threads will be at least the value of the parallelism parameter.
   * @param items The items to be processed. Should be independent of each other, and must be thread safe for concurrent handling by the supplied processor.
   * @param processor The task processor. Must be thread safe. Ideally reacts to interruption requests.
   * @throws E In case that the processor failed on some item with some error.
   * @throws InterruptedException In case that the current thread is interrupted. Note that by the time the exception is thrown, all tasks are guaranteed to be canceled/complete. 
   */
  public static <T, E extends Exception> void runFailable(ExecutorService ex, Collection<T> items, FailableInterruptibleConsumer<? super T, E> processor) throws E, InterruptedException {
    internalRun(ex, items, processor);
  }

  /**
   * Processed the items in parallel by running a given processor on them. 
   * Spawns threads according to the supplied parallelism level.
   * The processor here is allowed to throw checked exceptions, of type E.
   * 
   * Notes:
   * 1) In case of an error while processing one of the items, the method tries to exit as soon as possible. 
   * 2) The method returns only after making sure there aren't any pending items.
   * 3) It follows from (1) and (2) that the processor provided here should preferably be interruptible. This way we can exit faster when an error occurs in any of the items.
   * 
   * @param items The items to be processed. Should be independent of each other, and must be thread safe for concurrent handling by the supplied processor.
   * @param parallelism The number of threads to process the items
   * @param processor The task processor. Must be thread safe. Ideally reacts to interruption requests.
   * @throws RuntimeException In case of an unchecked error while running the processor on some item
   * @throws InterruptedException In case that the current thread is interrupted. Note that by the time the exception is thrown, all tasks are guaranteed to be canceled/complete.
   */
  public static <T, E extends Exception> void runFailable(Collection<T> items, int parallelism, FailableInterruptibleConsumer<? super T, E> processor) throws E, InterruptedException {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(ParallelTaskProcessor.class.getSimpleName() + "-%d").build();
    ExecutorService ex = Executors.newFixedThreadPool(parallelism, tf);
    try {
      internalRun(ex, items, processor);
    } finally {
      ex.shutdownNow(); // No need to await for termination, since we have the guarantee that the method doesn't return before all tasks complete
    }
  }

  /**
   * Processed the items in parallel, by running a given processor on them, using the provided executor.
   * The processor here is not allowed to throw checked exceptions.
   * 
   * Notes:
   * 1) In case of an error while processing one of the items, the method tries to exit as soon as possible. 
   * 2) The method returns only after making sure there aren't any pending items.
   * 3) It follows from (1) and (2) that the processor provided here should preferably be interruptible. This way we can exit faster when an error occurs in any of the items.
   * 
   * @param ex The executor to use for running the processor on. It is recommended that the number of background threads will be at least the value of the parallelism parameter.
   * @param items The items to be processed. Should be independent of each other, and must be thread safe for concurrent handling by the supplied processor.
   * @param processor The task processor. Must be thread safe. Ideally reacts to interruption requests.
   * @throws RuntimeException In case of an unchecked error while running the processor on some item
   * @throws InterruptedException In case that the current thread is interrupted. Note that by the time the exception is thrown, all tasks are guaranteed to be canceled/complete.
   */
  public static <T> void run(ExecutorService ex, Collection<T> items, Consumer<? super T> processor) throws InterruptedException {
    ParallelTaskProcessor.<T, RuntimeException>internalRun(ex, items, FailableInterruptibleConsumer.fromConsumer(processor));
  }

  /**
   * Processed the items in parallel by running a given processor on them. 
   * Spawns threads according to the supplied parallelism level.
   * The processor here is not allowed to throw checked exceptions.
   * 
   * Notes:
   * 1) In case of an error while processing one of the items, the method tries to exit as soon as possible. 
   * 2) The method returns only after making sure there aren't any pending items.
   * 3) It follows from (1) and (2) that the processor provided here should preferably be interruptible. This way we can exit faster when an error occurs in any of the items.
   * 
   * @param items The items to be processed. Should be independent of each other, and must be thread safe for concurrent handling by the supplied processor.
   * @param parallelism The number of threads to process the items
   * @param processor The task processor. Must be thread safe. Ideally reacts to interruption requests.
   * @throws RuntimeException In case of an unchecked error while running the processor on some item
   * @throws InterruptedException In case that the current thread is interrupted. Note that by the time the exception is thrown, all tasks are guaranteed to be canceled/complete.
   */
  public static <T> void run(Collection<T> items, int parallelism, Consumer<? super T> processor) throws RuntimeException, InterruptedException {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(ParallelTaskProcessor.class.getSimpleName() + "-%d").build();
    ExecutorService ex = Executors.newFixedThreadPool(parallelism, tf);
    try {
      ParallelTaskProcessor.<T, RuntimeException>internalRun(ex, items, FailableInterruptibleConsumer.fromConsumer(processor));
    } finally {
      ex.shutdownNow(); // No need to await for termination, since we have the guarantee that the method doesn't return before all tasks complete
    }
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Exception> void internalRun(ExecutorService ex, Collection<T> items, FailableInterruptibleConsumer<? super T, E> processor) throws E, InterruptedException {
    // Submit all tasks
    ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(ex);
    List<Future<?>> futures = new ArrayList<>(items.size());
    for (T item : items) {
      ItemsProcessorTask<T> task = new ItemsProcessorTask<>(item, processor);
      futures.add(ecs.submit(task));
    }
           
    // Wait for all tasks to terminate
    // The completion service allows us to detect a failure as soon as possible
    try {
      for (int i = 0; i < items.size(); i++) {
        ecs.take().get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Re-set the interrupted flag
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException)cause;
      } 
      if (cause instanceof InterruptedException) { // A task is interrupted but not by us
        throw (InterruptedException)cause;
      } 
      if (cause instanceof Error) { // Best effort to throw the error. Otherwise we'll have ClassCastException in the next line when casting to E
        throw (Error)cause;
      } 
      throw (E)cause;
    } finally {
      // Cancel all (no effect if already terminated)
      for (Future<?> f : futures) {
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
      
      // Wait for all tasks to terminate before exiting
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (CancellationException | ExecutionException e) {
          // ignore. We want to make sure all tasks terminate before exiting
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Re-set the interrupted flag    
        }
      }
      
      if (Thread.interrupted()) { // We intentionally use Thread.interrupted(), in order to clear the interruption flag before invoking the InterruptedException.
                                  // Otherwise the flag will still be on when the caller catches the exception (!)
        throw new InterruptedException();
      }
    }
  }

  private static class ItemsProcessorTask<T> implements Callable<Void> {
    private final T item;
    private final FailableInterruptibleConsumer<? super T, ?> consumer;

    public ItemsProcessorTask(T item, FailableInterruptibleConsumer<? super T, ?> consumer) {
      this.item = item;
      this.consumer = consumer;
    }
    
    @Override
    public Void call() throws Exception {
      consumer.accept(item);
      return null;
    }
  }
}
