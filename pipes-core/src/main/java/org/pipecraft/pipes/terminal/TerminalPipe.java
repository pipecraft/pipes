package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.BasePipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.InternalPipeException;

/**
 * Base class for terminal pipes, i.e. pipes that act as sinks and don't produce items.
 * 
 * See also the other types of {@link BasePipe} : {@link AsyncPipe} and {@link TerminalPipe}.
 * 
 * Guidelines:
 * 
 * 1. start() calls on terminal pipes block until all data is consumed successfully, an interruption occurs or an error occurs.
 * 2. start() is declared as interruptible, and is encouraged to be implemented accordingly.
 * 3. Terminal pipes are closeable. Make sure to dispose of resources, including closing all input pipes. Close operation should be idempotent.
 * 4. Constructors shouldn't fetch anything from input pipes. The data processing should take place in the start() method only.
 * 5. Pipes support progress tracking for monitoring iteration progress. Progress may be queried by another thread. The default implementation reports completion immediately (progress = 1.0),
 *    but implementations may choose to override it and increase progress gradually reflecting the actual progress while the start() method is running. 
 * 6. Thread safety:
 *    6.1. Unless specified otherwise, terminal pipes aren't thread safe (can't call close from another thread). 
 *    6.2. If the implementation exposes state through getters, the data should be protected (e.g. defined as volatile), since it may be written/read by different threads.
 *    6.3. Constructors should assign final data members only
 *    6.4. Progress tracking and querying should always be implemented in a thread safe manner. It's possible that the thread querying the progress is not the one
 *          incrementing it. A typical implementation is to make the progress counter volatile.
 * 7. Error handling - in case of an error, the start() method should exit as soon as possible and throw the corresponding exception (PipeException/InterruptedException/runtime exception).
 *    Note that in case of {@link InternalPipeException}, the actual exception to throw is InternalPipeException.getRuntimeException(), since it indicates a bug and not an ordinary
 *    PipeException. 
 * 
 * @author Eyal Schneider
 */
public abstract class TerminalPipe implements BasePipe {

  @Override
  public float getProgress() {
    return 1.0f;
  }

}
