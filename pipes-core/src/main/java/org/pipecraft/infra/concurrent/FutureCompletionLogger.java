package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Future;

/**
 * To be set as a listener on {@link ListenableFuture}s. 
 * This is useful in order to capture their termination results(either result object on success or exception in case of failure), and log accordingly.
 * Specially useful for tracking success/failure on futures which no thread block on (fire and forget scenarios).
 * 
 * @author Eyal Schneider
 */
public class FutureCompletionLogger extends FutureCompletionHandler<Object> {

  /**
   * Constructor
   * 
   * @param logger The logger to write to
   * @param successLevel The logging level to use when logging successful completion. Use null for no logging.
   * @param errLevel The logging level to use when logging failed completion. Use null for no logging.
   * @param actionName The name of the action
   * @param future The future object to test for errors. 
   **/
  protected FutureCompletionLogger(org.slf4j.Logger logger, org.slf4j.event.Level successLevel, org.slf4j.event.Level errLevel, String actionName, Future<?> future) {
    super(
        r -> logSLF4J(logger, successLevel, "Completed action |" + actionName + "| successfully."),
        e -> logSLF4JWithException(logger, errLevel, "Failed executing action |" + actionName + "|.", e),
        future
        );
  }

  /**
   * 
   * Adds a logging listener (SLF4J) to a given {@link ListenableFuture}. The logging occurs in the producer thread.
   * @param future The future to listen to
   * @param logger The logger to write to
   * @param successLevel The logging level to use when logging successful completion
   * @param errLevel The logging level to use when logging failed completion
   * @param actionName The name of the action
   */
  public static <T> void listenTo(ListenableFuture<T> future, org.slf4j.Logger logger, org.slf4j.event.Level successLevel, org.slf4j.event.Level errLevel, String actionName) {
    future.addListener(new FutureCompletionLogger(logger, successLevel, errLevel, actionName, future), MoreExecutors.directExecutor());
  }

  /**
   * 
   * Adds a logging listener (SLF4J) on error events to a given {@link ListenableFuture}. The logging occurs in the producer thread.
   * @param future The future to listen to
   * @param logger The logger to write to
   * @param level The logging level to use when logging failed completion
   * @param actionName The name of the action
   */
  public static <T> void listenToError(ListenableFuture<T> future, org.slf4j.Logger logger, org.slf4j.event.Level level, String actionName) {
    listenTo(future, logger, null, level, actionName);
  }

  private static void logSLF4J(org.slf4j.Logger logger, org.slf4j.event.Level level, String msg) {
    if (level != null) {
      switch (level) {
        case TRACE: logger.trace(msg); break;
        case DEBUG: logger.debug(msg); break;
        case INFO: logger.info(msg); break;
        case WARN: logger.warn(msg); break;
        case ERROR: logger.error(msg); break;
      }
    }
  }
  
  private static void logSLF4JWithException(org.slf4j.Logger logger, org.slf4j.event.Level level, String msg, Throwable exception) {
    if (level != null) {
      switch (level) {
        case TRACE: logger.trace(msg, exception); break;
        case DEBUG: logger.debug(msg, exception); break;
        case INFO: logger.info(msg, exception); break;
        case WARN: logger.warn(msg, exception); break;
        case ERROR: logger.error(msg, exception); break;
      }
    }
  }

}