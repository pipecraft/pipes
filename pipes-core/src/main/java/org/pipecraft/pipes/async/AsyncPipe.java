package org.pipecraft.pipes.async;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.pipecraft.pipes.BasePipe;
import org.pipecraft.pipes.exceptions.InternalPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe that produces items asynchronously.
 * It "publishes" the produced items through a registered listener.
 * Async pipes may create and own a set of threads for this purpose, but may alternatively run on the threads of upstream async pipes.   
 * 
 * Guidelines for implementors / users of async pipes:
 * 1. Unless specified otherwise by the implementation, items don't have any particular order, and the order may even be non-deterministic.
 * 2. The listener may be invoked from multiple threads
 * 3. An item is "published" downstream by the pipe by invoking the listener.next(T) method (see notifyNext() method)
 * 4. When the pipe is done generating all items successfully, it should invoke the listener.done() method, exactly once (see notifyDone()). 
 * 5. When the pipe is done generating items with an error, it should invoke the listener.error(e) method, exactly once (see notifyError()).
 * 6. An async pipe P being notified by the upstream pipe (either sync or async) about an error should simply propagate it by firing an error notification to the downstream pipe. 
 * 7. Once a call to listener.done() or listener.error(e) starts, no other listener method should be invoked.
 *    More specifically, using JMM nomenclature, all actions in listener.next() calls must happen-before the actions in listener.done() / listener.error(). This means that
 *    the implementation should add some synchronization tool to guarantee it (for example this is done by the pipe waiting for all its owned threads to terminate, either using
 *    Thread.join() or CountDownLatch counting the number of threads exiting their main loop).
 *    While the done()/error() exclusiveness is guaranteed by the framework (provided that notifyError() and notifyDone() are used),
 *    the happens-before property above is part of the responsibility of the specific pipe implementation.
 * 8. Implementation of start() method should start up the notification mechanism. The call should exit as soon as possible, leaving all notifications (next, done, error)
 *    to other threads. 
 * 9. A call to close() should stop the item generation mechanism as soon as possible (best effort), and block until stopping is complete. After the method exists normally,
 *    there should be no more notifications. As with any pipe, the call should also trigger a close operation on the immediate upstream pipes.
 *    For proper lifecycle tracking, the implementation should call the super's close() implementation.
 * 10. The close() method may be called from a different thread than the one invoking the start() method. In other words, async pipes must be thread safe.
 * 11. For consistency and interoperability with sync pipes - items produced by async pipes should never be null.
 * 12. Async pipes owning threads must add a final barrier protecting each thread from terminating with unexpected unchecked errors. In case of such errors, the pipe should
 *     report an error (notifyError()), using the {@link InternalPipeException} as a wrapper.
 *
 * @param <T> The data type of items produced by the pipe
 *
 * @author Eyal Schneider
 */
public abstract class AsyncPipe<T> implements BasePipe {
  public enum Status {WORKING, CLOSED, DONE, ERROR}
  
  private volatile AsyncPipeListener<? super T> listener;
  private final AtomicReference<Status> status = new AtomicReference<>(Status.WORKING); 
  
  /**
   * Sets the listener on produced items and other lifecycle events. Must be called prior to start().
   * @param listener The listener to register
   */
  public void setListener(AsyncPipeListener<? super T> listener) {
    this.listener = listener;
  }
  
  /**
   * Fires the event indicating a new item. 
   * @param item The new item being produced by the pipe
   * @throws InterruptedException In case that the thread is interrupted
   */
  protected void notifyNext(T item) throws InterruptedException {
    try {
      listener.next(item);
    } catch (PipeException e) {
      notifyError(e); // A downstream pipe reacting with an error to notifyNext is automatically notified back with an error notification,
                      // to make sure everything is shutdown.
    }
  }
  
  /**
   * Fires the event indicating a successful end of item generation
   * @throws InterruptedException in case that the thread is interrupted
   */
  protected void notifyDone() throws InterruptedException {
    if (status.compareAndSet(Status.WORKING, Status.DONE)) { // Guarantees at most one of done/error events, 
                                                             // and also that DONE can only come after WORKING state.
      listener.done();
    }
  }
  
  /**
   * Fires the event indicating a non-successful termination of the pipe's work
   * @param e The error details
   * 
   * @throws InterruptedException In case that the thread is interrupted
   */
  protected void notifyError(PipeException e) throws InterruptedException {
    if (status.compareAndSet(Status.WORKING, Status.ERROR)) { // Guarantees at most one of done/error events
                                                              // and also that ERROR can only come after WORKING state.
      listener.error(e);
    }
  }
  
  /**
   * @return The current pipe status: 
   * WORKING - The pipe is about to start working, or during work
   * CLOSED - The pipe has been closed
   * DONE - The pipe terminated normally
   * ERROR - The pipe terminated with error
   * 
   * The lifecycle flow is:
   * 
   * WORKING ---> CLOSED
   *   |            /|\
   *  \|/            |
   *   ----->DONE--->|
   *   |             |
   *   ----->ERROR-->|
   */
  public Status getStatus() {
    return status.get();
  }

  @Override
  public void close() throws IOException {
    status.set(Status.CLOSED);
  }
}
