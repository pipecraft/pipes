package org.pipecraft.infra.io;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.pipecraft.infra.monitoring.JsonMonitorable;
import org.pipecraft.infra.concurrent.FailableInterruptibleRunnable;
import org.pipecraft.infra.concurrent.FailableInterruptibleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.minidev.json.JSONObject;

/**
 * Runs a {@link FailableInterruptibleRunnable} or {@link FailableInterruptibleSupplier} with configurable retries.
 * Thread safe as long as the runnables passed to it are thread safe.
 * 
 * This class is Monitorable, and provides the following metrics:
 * - invocations - Total number of API invocations
 * - failedInvocations - Total number of invocations ultimately failing, after retries
 * - tries - Total number of attempts
 * 
 * @author Eyal Schneider
 */
public class Retrier implements JsonMonitorable {
  private static final Logger logger = LoggerFactory.getLogger(Retrier.class);
  
  private static final int DEFAULT_INITIAL_WAIT_TIME_MS = 1000;
  private static final double DEFAULT_WAIT_TIME_FACTOR = 2.0;
  private static final int DEFAULT_TIMES_TO_TRY = 4;

  private final AtomicInteger invocationsCount = new AtomicInteger(); // Total number of API invocations
  private final AtomicInteger failedInvocationsCount = new AtomicInteger(); // Total number of invocations ultimately failing, after reties
  private final AtomicInteger triesCount = new AtomicInteger(); // Total number of tries
  private final int initialWaitTimeMs;
  private final double waitTimeFactor;
  private final int timesToTry;
  
  /**
   * Constructor
   * 
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds. Must be non-negative.
   * @param waitTimeFactor The factor by which wait times increase. Should be greater than 1.0.
   * @param timesToTry The maximum times to attempt the run. Should be greater than 1.
   */
  public Retrier(int initialWaitTimeMs, double waitTimeFactor, int timesToTry) {
    this.initialWaitTimeMs = initialWaitTimeMs;
    this.waitTimeFactor = waitTimeFactor;
    this.timesToTry = timesToTry;
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor, and using only {@link RuntimeException} and {@link Error} as early exit errors.
   * @param toRun The failable runnable to run
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   */
  public <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun) throws E, InterruptedException {
    run(toRun, e -> false);
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor, and using only {@link RuntimeException} and {@link Error} as early exit errors.
   * @param toRun The failable supplier to run
   * @return The value produced by the supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   */
  public <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun) throws E, InterruptedException {
    return run(toRun, e -> false);
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor
   * @param toRun The failable runnable to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   */
  public <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun, Predicate<E> terminalErrorPredicate) throws E, InterruptedException {
    run((FailableInterruptibleSupplier<Void, E>)(() -> {toRun.run(); return null;}) , terminalErrorPredicate);
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor
   * @param toRun The failable runnable to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   */
  public <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun, Collection<Class<? extends E>> terminalErrorTypes) throws E, InterruptedException {
    run(toRun, e -> isAnyOf(e, terminalErrorTypes));
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor
   * @param toRun The failable supplier to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @return The value produced by the supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun, Collection<Class<? extends E>> terminalErrorTypes) throws E, InterruptedException {
    return run(toRun, e -> isAnyOf(e, terminalErrorTypes));
  }

  /**
   * Runs the given task with the retry configuration as defined in the constructor
   * @param toRun The failable runnable to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun, Predicate<E> terminalErrorPredicate) throws E, InterruptedException {
    int attempts = 0;
    invocationsCount.incrementAndGet();
    boolean success = false;
    T result;
    try {
      while (true) {
        try {
          attempts++;
          triesCount.incrementAndGet();
          result = toRun.get();
          success = true;
          return result;
        } catch(RuntimeException | Error | InterruptedException x) {
            throw x; 
        } catch (Exception e) {
          if(terminalErrorPredicate.test((E) e)) {
            throw e;
          }
          logger.debug("Task failed after " + attempts + " attempt/s.", e);
          if (attempts >= timesToTry) {
            throw e;
          }       
          TimeUnit.MILLISECONDS.sleep((long)(initialWaitTimeMs * Math.pow(waitTimeFactor, attempts - 1)));
        }
      }
    } finally {
      if (!success) {
        failedInvocationsCount.incrementAndGet();
      }
    }
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable runnable to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void runWithDefaults(FailableInterruptibleRunnable<E> toRun, Predicate<E> terminalErrorPredicate) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    retrier.run(toRun, terminalErrorPredicate);
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable supplier to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T runWithDefaults(FailableInterruptibleSupplier<T, E> toRun, Predicate<E> terminalErrorPredicate) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    return retrier.run(toRun, terminalErrorPredicate);
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable runnable to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void runWithDefaults(FailableInterruptibleRunnable<E> toRun, Collection<Class<? extends E>> terminalErrorTypes) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    retrier.run(toRun, terminalErrorTypes);
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable supplier to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T runWithDefaults(FailableInterruptibleSupplier<T, E> toRun, Collection<Class<? extends E>> terminalErrorTypes) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    return retrier.run(toRun, terminalErrorTypes);
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable runnable to run
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void runWithDefaults(FailableInterruptibleRunnable<E> toRun) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    retrier.run(toRun);
  }

  /**
   * Runs the given task with the default retry configuration
   * @param toRun The failable runnable to run
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T runWithDefaults(FailableInterruptibleSupplier<T, E> toRun) throws E, InterruptedException {
    Retrier retrier = new Retrier(DEFAULT_INITIAL_WAIT_TIME_MS, DEFAULT_WAIT_TIME_FACTOR, DEFAULT_TIMES_TO_TRY);
    return retrier.run(toRun);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable runnable to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds
   * @param waitTimeFactor The factor by which wait times increase
   * @param timesToTry The maximum times to attempt the run
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun, Predicate<E> terminalErrorPredicate, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    retrier.run(toRun, terminalErrorPredicate);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable supplier to run
   * @param terminalErrorPredicate A predicate indicating what kind of exceptions should not trigger retries, and should exit immediately instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds
   * @param waitTimeFactor The factor by which wait times increase
   * @param timesToTry The maximum times to attempt the run
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun, Predicate<E> terminalErrorPredicate, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    return retrier.run(toRun, terminalErrorPredicate);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable runnable to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds
   * @param waitTimeFactor The factor by which wait times increase
   * @param timesToTry The maximum times to attempt the run
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun, Collection<Class<? extends E>> terminalErrorTypes, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    retrier.run(toRun, terminalErrorTypes);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable supplier to run
   * @param terminalErrorTypes A set of exceptions that should not trigger retries, and should casue immediate exit instead.
   * Note that {@link RuntimeException}, {@link Error} and {@link InterruptedException} are always handled as terminal errors.
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds
   * @param waitTimeFactor The factor by which wait times increase
   * @param timesToTry The maximum times to attempt the run
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun, Collection<Class<? extends E>> terminalErrorTypes, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    return retrier.run(toRun, terminalErrorTypes);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable runnable to run
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds. Must be non-negative.
   * @param waitTimeFactor The factor by which wait times increase. Should be greater than 1.0.
   * @param timesToTry The maximum times to attempt the run. Should be greater than 1.
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <E> The type of the checked error that can be produced while executing the runnable
   */
  public static <E extends Exception> void run(FailableInterruptibleRunnable<E> toRun, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    retrier.run(toRun);
  }

  /**
   * Runs the given task with the given retry configuration
   * @param toRun The failable supplirt to run
   * @param initialWaitTimeMs The initial wait time after the first failure, in milliseconds. Must be non-negative.
   * @param waitTimeFactor The factor by which wait times increase. Should be greater than 1.0.
   * @param timesToTry The maximum times to attempt the run. Should be greater than 1.
   * @return The value produced by the given supplier
   * @throws E In case of a terminal error or in case that all retries failed
   * @throws InterruptedException In case of an interruption during the retry loop
   * @param <T> The data type of the produced output
   * @param <E> The type of the checked error that can be produced while computing the output
   */
  public static <T, E extends Exception> T run(FailableInterruptibleSupplier<T, E> toRun, int initialWaitTimeMs, double waitTimeFactor, int timesToTry) throws E, InterruptedException {
    Retrier retrier = new Retrier(initialWaitTimeMs, waitTimeFactor, timesToTry);
    return retrier.run(toRun);
  }

  private static <E extends Exception> boolean isAnyOf(E e, Collection<Class<? extends E>> terminalErrorTypes) {
    for (Class<? extends E> exceptionType : terminalErrorTypes) {
      if (exceptionType.isInstance(e)) {
        return true;
      }
    }
    return false;
  }


  @Override
  public JSONObject getOwnMetrics() {
    JSONObject json = new JSONObject();
    json.put("invocations", invocationsCount.get());
    json.put("failedInvocations", failedInvocationsCount.get());
    json.put("tries", triesCount.get());
    return json;
  }


  @Override
  public Map<String, ? extends JsonMonitorable> getChildren() {
    return Collections.emptyMap();
  }

}
