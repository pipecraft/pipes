package org.pipecraft.infra.concurrent;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A consumer that may fail with a checked exception during the execution of the accept(..) method.
 * 
 * @param <T> The type of the data being consumed
 * @param <E> The exception type
 * 
 * @author Eyal Schneider
 */
@FunctionalInterface
public interface FailableConsumer <T,E extends Exception> {
  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   * @throws E in case of an error during the data consumption
   */
  void accept(T t) throws E;
  
  /**
   * Similar to andThen method of {@link java.util.function.Consumer}
   * @param after the operation to perform after this operation
   * @return a composed {@link FailableConsumer} that performs in sequence this operation followed by the after operation
   */
  default FailableConsumer<T,E> andThen(FailableConsumer<? super T, ? extends E> after) {
    Objects.requireNonNull(after);
    return (T t) -> { accept(t); after.accept(t); };
  }
  
  /**
   * Utility method for wrapping a consumer with a failable consumer
   * @param consumer The consumer to wrap
   * @return The failable consumer (which effectively never throws checked exceptions)
   * @param <T> The type of the data being consumed
   * @param <E>  The exception type
   */
  static <T, E extends Exception> FailableConsumer<T,E> fromConsumer(Consumer<T> consumer) {
    return new FailableConsumer<T, E>() {
      @Override
      public void accept(T t) throws E {
        consumer.accept(t);
      }};
  }
}
