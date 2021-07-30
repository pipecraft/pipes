package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.Futures;

/**
 * A checked listenable future that returns a predefined value or throws a predifined exception.
 * 
 * @author Eyal Schneider
 *
 * @param <V> The future's value data type
 */
public class ImmediateFuture<V, E extends Exception> extends AbstractCheckedFuture<V, E> {
  private ImmediateFuture(V result) {
    super(Futures.immediateFuture(result));
  }

  private ImmediateFuture(E exception) {
    super(Futures.immediateFailedFuture(exception));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected E map(Exception e) {
    return (E)e;
  }
  
  /**
   * @param value A value to be returned by the future
   * @return The listenable checked future programmed with the given value
   */
  public static <V, E extends Exception> CheckedFuture<V, E> ofValue(V value) {
    return new ImmediateFuture<V, E>(value);
  }
  
  /**
   * @param exception The exception to throw
   * @return The listenable checked future programmed with the given exception
   */
  public static <V, E extends Exception> CheckedFuture<V, E> ofError(E exception) {
    return new ImmediateFuture<V, E>(exception);
  }
}
