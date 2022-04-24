package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * To be set as a listener on {@link ListenableFuture}s. 
 * This is useful in order to capture their termination results(either result object on success or exception in case of failure), and react accordingly.
 * Specially useful for tracking success/failure on futures which no thread block on (fire and forget scenarios).
 *
 * @param <T> The type of the value produced in case of success
 *
 * @author Eyal Schneider
 */
public class FutureCompletionHandler <T> implements Runnable {
  private final Future<? extends T> future;
  private final Consumer<? super T> successListener;
  private final Consumer<Throwable> errorListener;

  /**
   * Protected constructor
   * 
   * @param successListener The listener to be triggered when the future's data is produced successfully
   * @param errorListener The listener to be triggered when the future's computation terminated with an error
   * @param future The future object to test for errors. Should be the same future this object is set as listener for.
   **/
  protected FutureCompletionHandler(Consumer<? super T> successListener, Consumer<Throwable> errorListener, Future<? extends T> future) {
    this.successListener = successListener;
    this.errorListener = errorListener;
    this.future = future;
  }
  
  @Override
  public void run() {
    T res;
    try {
      res = future.get();
    } catch (Throwable e) {
      if (e instanceof ExecutionException) {
        e = e.getCause();
      }
      errorListener.accept(e);
      return;
    }
    successListener.accept(res);
  }
  
  /**
   * Adds a completion listener to a given {@link ListenableFuture}. The listener runs on the producer thread.
   * @param future The future to listen to
   * @param successListener The listener to be triggered when the future's data is produced successfully 
   * @param errorListener The listener to be triggered when the future's computation terminated with an error
   * @param <T> The type of the value produced in case of success
   */
  public static <T> void listenTo(ListenableFuture<T> future, Consumer<? super T> successListener, Consumer<Throwable> errorListener) {
    listenTo(future, successListener, errorListener, MoreExecutors.directExecutor());
  }

  /**
   * Adds a completion listener to a given {@link ListenableFuture}
   * @param future The future to listen to
   * @param successListener The listener to be triggered when the future's data is produced successfully 
   * @param errorListener The listener to be triggered when the future's computation terminated with an error
   * @param ex The executor to run the callbacks on
   * @param <T> The type of the value produced in case of success
   */
  public static <T> void listenTo(ListenableFuture<T> future, Consumer<? super T> successListener, Consumer<Throwable> errorListener, Executor ex) {
    future.addListener(new FutureCompletionHandler<>(successListener, errorListener, future), ex);
  }

  /**
   * Adds a successful completion listener to a given {@link ListenableFuture}. The listener runs on the producer thread.
   * @param future The future to listen to
   * @param successListener The listener to be triggered when the future's data is produced successfully
   * @param <T> The type of the value produced in case of success
   */
  public static <T> void listenToSuccess(ListenableFuture<T> future, Consumer<? super T> successListener) {
    listenToSuccess(future, successListener, MoreExecutors.directExecutor());
  }

  /**
   * Adds a successful completion listener to a given {@link ListenableFuture}
   * @param future The future to listen to
   * @param successListener The listener to be triggered when the future's data is produced successfully 
   * @param ex The executor to run the success callback on
   * @param <T> The type of the value produced in case of success
   */
  public static <T> void listenToSuccess(ListenableFuture<T> future, Consumer<? super T> successListener, Executor ex) {
    future.addListener(new FutureCompletionHandler<>(successListener, e -> {}, future), ex);
  }

  /**
   * Adds an error listener to a given {@link ListenableFuture}. The listener runs on the producer thread.
   * @param future The future to listen to
   * @param errorListener The listener to be triggered when the future's computation terminated with an error
   */
  public static void listenToError(ListenableFuture<?> future, Consumer<Throwable> errorListener) {
    listenToError(future, errorListener, MoreExecutors.directExecutor());
  }

  /**
   * Adds an error listener to a given {@link ListenableFuture}
   * @param future The future to listen to
   * @param errorListener The listener to be triggered when the future's computation terminated with an error
   * @param ex The executor to run the error callback on
   */
  public static void listenToError(ListenableFuture<?> future, Consumer<Throwable> errorListener, Executor ex) {
    future.addListener(new FutureCompletionHandler<>(r -> {}, errorListener, future), ex);
  }

}