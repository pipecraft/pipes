package org.pipecraft.infra.concurrent;

import java.util.function.Supplier;

/**
 * A supplier that is interruptible and may also fail with a checked exception during the execution of the get() method.
 *
 * @param <T> The type of the data being returned
 * @param <E> The exception type
 * @author Shai Barad
 */
@FunctionalInterface
public interface FailableInterruptibleSupplier<T, E extends Exception> {

  /**
   * Gets a result.
   *
   * @return the result
   * @throws E the checked exception indicating a failure to produce the value
   * @throws InterruptedException In case that the current thread is interrupted while producing the value
   */
  T get() throws E, InterruptedException;

  /**
   * Utility method for wrapping a supplier with a failable supplier
   *
   * @param supplier The supplier to wrap
   * @return The failable supplier (which effectively never throws checked exceptions)
   * @param <T> The type of the data being returned
   * @param <E> The exception type
   */
  static <T, E extends Exception> FailableInterruptibleSupplier<T, E> fromSupplier(Supplier<T> supplier) {
    return supplier::get;
  }
}
