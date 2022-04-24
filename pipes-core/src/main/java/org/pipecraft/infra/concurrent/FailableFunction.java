package org.pipecraft.infra.concurrent;

import java.util.Objects;

/**
 * A function that may fail with a checked exception during the execution of the apply(..) method.
 *
 * @param <T> The type of the input
 * @param <R> The type of the output
 * @param <E> The exception type
 *
 * @author Eyal Schneider
 */
@FunctionalInterface
public interface FailableFunction <T, R, E extends Exception> {
  /**
   * Applies the function
   *
   * @param v the input argument
   * @return the function result
   * @throws E in case of an error
   */
  R apply(T v) throws E;
  
  /**
   * Similar to Function.compose(..), but works with {@link FailableFunction}
   * @param before the function to apply before this function is applied
   * @return a composed {@link FailableFunction} that first applies the before function and then applies this function
   * @param <V> The input type of the supplied function to be applied first
   */
  default <V> FailableFunction<V, R, E> compose(FailableFunction<? super V, ? extends T, E> before) {
    Objects.requireNonNull(before);
    return (V v) -> apply(before.apply(v));
  }

  /**
   * Similar to Function.andThen(..), but works with {@link FailableFunction}
   * @param after the {@link FailableFunction} to apply after this function is applied
   * @return a composed function that first applies this function and then applies the after function
   * @param <V> The output type of the supplied function
   */
  default <V> FailableFunction<T, V, E> andThen(FailableFunction<? super R, ? extends V, E> after) {
    Objects.requireNonNull(after);
    return (T t) -> after.apply(apply(t));
  }

  /**
   * Similar to Function.identity(), but returns a {@link FailableFunction}
   * @return a function that always returns its input argument.
   * @param <T> The input/output type
   * @param <E> The error type
   */
  static <T, E extends Exception> FailableFunction<T, T, E> identity() {
    return t -> t;
  }
}
