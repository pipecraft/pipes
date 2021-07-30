package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.infra.concurrent.FailableConsumer;
import org.pipecraft.pipes.exceptions.InternalPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * Consumes the contents of an {@link AsyncPipe}, blocking in the start() method until all data is read.
 * This class is similar to {@link ConsumerPipe}, but intended for async pipes.
 * The close() method should be called by the same thread calling start().
 * 
 * @param <T> The input item type
 * 
 * @author Eyal Schneider
 */
public class AsyncConsumerPipe<T> extends TerminalPipe {
  private final AsyncPipe<T> inputPipe;
  private volatile PipeException error; // Records the error sent by the upstream pipe, if any
  private final CountDownLatch endLatch = new CountDownLatch(1);

  /**
   * Constructor
   * 
   * @param inputPipe The pipe to consume
   * @param itemAction The action to perform on all items. Legitimate errors should be wrapped by PipeException.
   * Errors cause pipeline termination. Must be thread safe!
   * @param terminationAction An action to perform once all input items have been consumed. Runs once, upon a successful iteration termination only. 
   */
  public AsyncConsumerPipe(AsyncPipe<T> inputPipe, FailableConsumer<? super T, PipeException> itemAction, Runnable terminationAction) {
    this.inputPipe = inputPipe;
    inputPipe.setListener(new AsyncPipeListener<>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        itemAction.accept(item);
      }

      @Override
      public void done() throws InterruptedException {
        terminationAction.run();
        endLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        error = e;
        endLatch.countDown();
      }
    });
  }

  /**
   * Constructor
   * 
   * @param inputPipe The pipe to consume
   * @param itemAction The action to perform on all items. Legitimate errors should be wrapped by PipeException.
   * Errors cause pipeline termination. Must be thread safe!
   */
  public AsyncConsumerPipe(AsyncPipe<T> inputPipe, FailableConsumer<? super T, PipeException> itemAction) {
    this(inputPipe, itemAction, () -> {});
  }

  /**
   * Constructor
   * 
   * @param inputPipe The pipe to consume
   */
  public AsyncConsumerPipe(AsyncPipe<T> inputPipe) {
    this(inputPipe, item -> {}, () -> {});
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    inputPipe.start();
    endLatch.await();
    if (error != null) {
      if (error instanceof InternalPipeException) {
        throw ((InternalPipeException) error).getRuntimeException(); // Transforming the wrapper to the actual runtime exception it should be
      }
      throw error;
    }
  }

  @Override
  public float getProgress() {
    return inputPipe.getProgress();
  }

  @Override
  public void close() throws IOException {
    endLatch.countDown();
    inputPipe.close();
  }

}
