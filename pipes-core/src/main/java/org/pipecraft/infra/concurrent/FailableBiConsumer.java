package org.pipecraft.infra.concurrent;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A bi-consumer that may fail with a checked exception during the execution of the accept(..) method.
 * 
 * @param <A> The type of the first argument being consumed
 * @param <B> The type of the second argument being consumed
 * @param <E> The exception type
 * 
 * @author Eyal Schneider
 */
@FunctionalInterface
public interface FailableBiConsumer <A, B, E extends Exception> {
  /**
   * Performs this operation on the given arguments.
   *
   * @param a argument #1
   * @param b argument #2
   * @throws E in case of an error during the data consumption
   */
  void accept(A a, B b) throws E;
  
  /**
   * Similar to andThen method of {@link java.util.function.Consumer}
   * @param after the operation to perform after this operation
   * @return a composed {@link FailableBiConsumer} that performs in sequence this operation followed by the after operation
   */
  default FailableBiConsumer<A, B, E> andThen(FailableBiConsumer<? super A, ? super B, ? extends E> after) {
    Objects.requireNonNull(after);
    return (a, b) -> { accept(a, b); after.accept(a, b); };
  }
  
  /**
   * Utility method for wrapping a bi-consumer with a failable bi-consumer
   * @param biConsumer The bi-consumer to wrap
   * @return The failable bi-consumer (which effectively never throws checked exceptions)
   * @param <X> The type of the first argument being consumed
   * @param <Y> The type of the second argument being consumed
   * @param <E> The exception type
   */
  static <X, Y, E extends Exception> FailableBiConsumer<X, Y, E> fromBiConsumer(BiConsumer<X, Y> biConsumer) {
    return new FailableBiConsumer<X, Y, E>() {
      @Override
      public void accept(X a, Y b) throws E {
        biConsumer.accept(a, b);
      }};
  }
}
