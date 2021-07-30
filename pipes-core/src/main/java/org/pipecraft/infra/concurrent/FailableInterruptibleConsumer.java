package org.pipecraft.infra.concurrent;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * An interreuptible consumer that may also fail with a checked exception during the execution of the accept(..) method.
 * 
 * @param T The type of the data being consumed
 * @param E The exception type
 * 
 * @author Eyal Schneider
 */
@FunctionalInterface
public interface FailableInterruptibleConsumer <T,E extends Exception> {
  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   */
  void accept(T t) throws E, InterruptedException;
  
  /**
   * Similar to andThen method of {@link java.util.function.Consumer}
   */
  default FailableInterruptibleConsumer<T,E> andThen(FailableInterruptibleConsumer<? super T, ? extends E> after) {
    Objects.requireNonNull(after);
    return (T t) -> { accept(t); after.accept(t); };
  }
  
  /**
   * Utility method for wrapping a consumer with a failable consumer
   * @param consumer The consumer to wrap
   * @return The failable consumer (which effectively never throws checked exceptions)
   */
  static <T, E extends Exception> FailableInterruptibleConsumer<T,E> fromConsumer(Consumer<T> consumer) {
    return new FailableInterruptibleConsumer<T, E>() {
      @Override
      public void accept(T t) throws E {
        consumer.accept(t);
      }};
  }
}
