package org.pipecraft.pipes.async;

import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A listener to be attached to {@link AsyncPipe}s.
 * Gets notified in 3 cases:
 * 1) When a pipe item is produced (analogous to a sync pipe returning some value when calling next())
 * 2) Upon successful termination (analogous to a sync pipe returning null in next() method)
 * 3) Upon error termination (analogous to a sync pipe throwing an exception during call to next())
 * 
 * @param <T> The data type the pipe works with
 * 
 * @author Eyal Schneider
 */
public interface AsyncPipeListener <T> {

  /**
   * Notifies that a new item has been produced by the async pipe. 
   * This method may be called by different threads, therefore should be implemented carefully. There's no guarantee regarding the order at which items are produced,
   * and it may even be non-deterministic.
   * 
   * @param item The produced item
   * 
   * @throws PipeException In case that the new item should trigger a pipeline error
   * @throws InterruptedException In case the thread is interrupted while handling the new item
   */
  void next(T item) throws PipeException, InterruptedException;

  /**
   * Indicates that the async pipe is done producing items, in a successfull manner.
   * This method is called at most once, and once called, it's guaranteed that no more calls to next(T) are expected. 
   * In addition, the implementation can assume that exactly one of error() / done() are called, but never both.
   * 
   * @throws InterruptedException In case the thread is interrupted while handling the done event
   */
  void done() throws InterruptedException;
  
  /**
   * Notifies the listener that an error has been detected by the async pipe during item generation.
   * The implementation can't assume that no further accept(..) calls will be made, but it can assume that exactly one of error() / done() are called, but never both.
   * @param e The exception
   * 
   * @throws InterruptedException In case the thread is interrupted while handling the done event
   */
  void error(PipeException e) throws InterruptedException;
}
