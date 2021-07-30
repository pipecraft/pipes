package org.pipecraft.pipes.sync;

import org.pipecraft.pipes.BasePipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.InternalPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * The interface to be implemented by synchronous pipes, which are passive and items are "pulled" from them.
 * See also the other types of {@link BasePipe} : {@link AsyncPipe} and {@link TerminalPipe}.
 * 
 * 1) A pipe takes as an input zero or more sync/async pipes, and generates an ordered stream of items, of a type and multiplicity depending on the implementation. 
 * 2) Pipes with no input pipes are called "source pipes"
 * 3) Pipes may be stateful
 * 4) Pipes should produce only PipeException errors during processing. Use specific subtypes where possible.
 * 5) Pipes are closeable. Make sure to dispose of resources, including closing all input pipes. Close operation should be idempotent.
 * 6) Constructors shouldn't fetch anything from input pipes. The start() method is intended for preparation actions, including possibly fetching of first item/s from the input pipes.
 * 7) Pipes are interruptible and are encouraged to be implemented accordingly.
 * 8) Pipes support progress tracking for monitoring iteration progress
 * 9) Thread safety:
 *    9.1. In general pipes aren't required to be fully thread safe by support multiple threads calling their public methods. 
 *         Unless specified otherwise, when consuming a pipe, it's important to make sure that the same thread which invokes start() is the one invoking next() / peek() and finally close() later.
 *    9.2. If the implementation exposes state through getters, the data should be protected (e.g. defined as volatile), since it may be written/read by different threads.
 *    9.3. Constructors should assign final data members only
 *    9.4. Progress tracking and querying should always be implemented in a thread safe manner. It's possible that the thread querying the progress is not the one
 *         incrementing it. A typical implementation is to make the progress counter volatile.
  * 10. Error handling - in case of sync pipes as input/s, a pipe should let the exceptions (PipeException/InterruptedException/runtime exceptions) propagate.
 *    In case that the input is async, the implementation must make sure to throwing the corresponding exception in the next call to next().     
 *    Note that in case of {@link InternalPipeException}, the actual exception to throw is InternalPipeException.getRuntimeException(), since it indicates a bug and not an ordinary
 *    PipeException. 
 * 
 * The typical lifecycle is: 
 * 
 * try (MyPipe pipe = new MyPipe(inputPipes, otherParams)) {
 *   pipe.start();
 * 
 *   ItemType next;
 *   while ((next = pipe.next()) != null) {
 *     foo(next);
 *   }
 * }
 *  
 * @param <T> The type of the ouput items
 *
 * @author Eyal Schneider
 */
public interface Pipe<T> extends BasePipe {
  
  /**
   * @return The next item in this pipe output, or null if the output end has been reached. May be a blocking operation.
   * @throws PipeException In case of pipe errors in this pipe or somewhere up-stream while trying to prepare next item to return.
   * @throws InterruptedException In case that the operation has been interrupted by another thread.
   */
  T next() throws PipeException, InterruptedException;
  
  /**
   * @return The next item in the pipe's output. Does not remove it, so next call to next() will return it.
   * @throws PipeException In case of pipe errors in this pipe or somewhere up-stream while trying to prepare next item to return.
   */
  T peek() throws PipeException;
}
