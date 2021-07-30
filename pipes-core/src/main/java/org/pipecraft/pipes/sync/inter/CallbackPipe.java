package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableRunnable;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.concurrent.FailableConsumer;

/**
 * A pipe invoking a callback method per item passed through it.
 * The callback is never invoked on null values which indicate end of input pipe.
 * Also supports a termination action, which is run exactly once when pipe termination is detected.
 * Pipe output is same as pipe input.
 * 
 * @param <T> The item data type
 *
 * @author Eyal Schneider
 */
public class CallbackPipe<T> extends DelegatePipe<T>{
  private final FailableConsumer<? super T, ? extends PipeException> itemCallback;
  private final FailableRunnable<? extends PipeException> terminationCallback;
  private boolean terminationCallbackInvoked;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param itemCallback The callback to invoke. Allows throwing a {@link PipeException} to signal pipeline termination.
   * @param terminationCallback A termination callback, executed once, when pipe termination is detected. Note that this callback can still fail the 
   * processing with {@link PipeException} if needed.
   * If input pipe isn't fully consumed, this callback won't be triggered.
   */
  public CallbackPipe(Pipe<T> input, FailableConsumer<? super T, ? extends PipeException> itemCallback, FailableRunnable<? extends PipeException> terminationCallback) {
    super(input);
    this.itemCallback = itemCallback;
    this.terminationCallback = terminationCallback;
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param itemCallback The callback to invoke 
   */
  public CallbackPipe(Pipe<T> input, FailableConsumer<? super T, ? extends PipeException> itemCallback) {
    this(input, itemCallback, () -> {});
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param terminationCallback A termination callback, executed once, when pipe termination is detected. 
   * If input pipe isn't fully consumed, this callback won't be triggered. 
   */
  public CallbackPipe(Pipe<T> input, FailableRunnable<? extends PipeException> terminationCallback) {
    this(input, r -> {}, terminationCallback);
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T next = getOriginPipe().next();
    if (next != null) {
      itemCallback.accept(next);
    } else if (!terminationCallbackInvoked) {
      runTermination();
    }
    return next;
  }

  @Override
  public T peek() throws PipeException {
    T next = getOriginPipe().peek();
    if (next == null && !terminationCallbackInvoked) {
      runTermination();
    }
    return next;
  }  

  private void runTermination() throws PipeException {
    terminationCallbackInvoked = true;
    terminationCallback.run();
  }
}
