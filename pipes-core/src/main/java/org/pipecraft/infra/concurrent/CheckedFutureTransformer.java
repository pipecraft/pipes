package org.pipecraft.infra.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A {@link CheckedFuture} decorator converting a checked future with value type S to a checked future with value type T.
 * Preserves the exception type.
 * 
 * @param S The source future type
 * @param T The target future type
 * @param E The type of the checked exception thrown by the checkedGet() methods
 * 
 * @author Eyal Schneider
 * 
 */
public class CheckedFutureTransformer<S, T, E extends Exception> implements CheckedFuture<T, E> {
  private final AbstractCheckedFuture<S, E> future;
  private final Function<S, T> mappingFunction;

  /**
   * Constructor
   * 
   * @param future the checked future to wrap and convert
   * @param mappingFunction Maps results of the original future's type S to the target type T
   */
  public CheckedFutureTransformer(AbstractCheckedFuture<S, E> future, Function<S,T> mappingFunction) {
    this.future = future;
    this.mappingFunction = mappingFunction;
  }
  
  /**
   * A simplified version of {@link Future#get()} which maps {@link ExecutionException} to a more indicative exception type
   *
   * @return the result of executing the future.
   * @throws InterruptedException When the waiting for completion is interrupted
   * @throws E when an execution exception is detected and mapped
   * @throws CancellationException if the computation was cancelled
   */
  public T checkedGet() throws InterruptedException, E {
    return mappingFunction.apply(future.checkedGet());
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
  public T checkedGet(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, E {
    return mappingFunction.apply(future.checkedGet(timeout, unit));
  }

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
  public T get() throws InterruptedException, ExecutionException {
    return mappingFunction.apply(future.get());
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return mappingFunction.apply(future.get(timeout, unit));
  }
}
