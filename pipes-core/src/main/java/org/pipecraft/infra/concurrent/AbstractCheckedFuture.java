package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An abstract decorator on {@link ListenableFuture} which adds checkedGet() methods which simplify Future's error handling.
 * Exceptions originating from the future computation are extracted from the {@link ExecutionException} wrapper,
 * and passed to the implementation's exception mapper, which is responsible for mapping to the proper exception of type E.
 * Unless really required, it's strongly recommended to handle wrapped runtime exceptions differently than checked exceptions.
 * They should be preserved and thrown unchanged, since they usually denote a bug.
 * 
 * This class implements the CheckedFuture interface, which is a replacement for Google's deprecated CheckedFuture.
 * 
 * @param V The future's output value type
 * @param E The type of the checked exception thrown by the checkedGet() methods
 * 
 * @author Eyal Schneider
 * 
 */
public abstract class AbstractCheckedFuture<V, E extends Exception> implements CheckedFuture<V, E> {
  private final ListenableFuture<V> future;

  /**
   * Constructor
   * 
   * @param future the listenable future to wrap and add the checked getters functionality to.
   */
  public AbstractCheckedFuture(ListenableFuture<V> future) {
    this.future = future;
  }
  
  /**
   * A simplified version of {@link Future#get()} which maps {@link ExecutionException} to a more indicative exception type
   *
   * @return the result of executing the future.
   * @throws InterruptedException When the waiting for completion is interrupted
   * @throws E when an execution exception is detected and mapped
   * @throws CancellationException if the computation was cancelled
   */
  public V checkedGet() throws InterruptedException, E {
    try {
      return future.get();
    } catch (ExecutionException e) {
      handleExecutionException(e);
      throw new IllegalStateException("Unreachable");
    }
  }

  /**
   * A simplified version of {@link Future#get(long, TimeUnit)} which maps {@link ExecutionException} to a more indicative exception type
   *
   * @param timeout The timeout to apply
   * @param unit The time unit of the timeout parameter
   * @return the result of executing the future.
   * @throws TimeoutException In case that the timeout period elapses
   * @throws InterruptedException When the waiting for completion is interrupted
   * @throws E when an execution exception is detected and mapped
   * @throws CancellationException if the computation was cancelled
   */
  public V checkedGet(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, E {
    try {
      return future.get(timeout, unit);
    } catch (ExecutionException e) {
      handleExecutionException(e);
      throw new IllegalStateException("Unreachable");
    }
  }

  /**
   * Maps a checked exception originating from the underlying computation into the standard exception type E.
   * In most cases when e is of type RuntimeException, it's recommended to simply throw it here,
   * to avoid hiding a severe error or presenting it as a legitimate error.
   * @param e The original exception
   * @return The mapped exception of type E
   */
  protected abstract E map(Exception e);
  
  @Override
  public void addListener(Runnable listener, Executor executor) {
    future.addListener(listener, executor);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return future.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, unit);
  }

  private void handleExecutionException(ExecutionException e) throws E {
    Throwable err = e.getCause();
    if (err instanceof Error) {
      throw (Error)err;
    }
    throw map((Exception)err);
  }
}
