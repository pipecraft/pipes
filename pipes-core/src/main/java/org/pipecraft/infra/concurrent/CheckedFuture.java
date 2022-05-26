package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Extends {@link ListenableFuture} and adds checkedGet() methods which simplify Future's error handling.
 * This interface is a replacement for Google's deprecated CheckedFuture.
 * 
 * @param <V> The future's output value type
 * @param <E> The type of the checked exception thrown by the checkedGet() methods
 * 
 * @author Eyal Schneider
 * 
 */
public interface CheckedFuture<V, E extends Exception> extends ListenableFuture<V> {
  /**
   * A simplified version of {@link Future#get()} which maps {@link ExecutionException} to a more indicative exception type
   *
   * @return the result of executing the future.
   * @throws InterruptedException When the waiting for completion is interrupted
   * @throws E when an execution exception is detected and mapped
   * @throws CancellationException if the computation was cancelled
   */
  V checkedGet() throws InterruptedException, E;

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
  V checkedGet(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, E;
}
