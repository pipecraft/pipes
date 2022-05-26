package org.pipecraft.infra.concurrent;

/**
 * A runnable that is interruptible and may also fail with a checked exception during the execution of the run() method.
 * 
 * @param <E> The exception type
 * 
 * @author Eyal Schneider
 */
@FunctionalInterface
public interface FailableInterruptibleRunnable <E extends Exception> {
  /**
   * Runs the runnable's task
   */
  void run() throws E, InterruptedException;
}
