package org.pipecraft.pipes.async.inter;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.SortedUnionPipe;
import org.pipecraft.pipes.utils.PipeUtils;

/**
 * An intermediate async pipe fed by a set of other async pipes.
 * This pipe passes on all events produced by the input pipes, on their original threads. It also takes care of detecting end of input properly. 
 * Serves in cases where we need a single async pipe as an input for some processing.
 * 
 * Similar to {@link SortedUnionPipe}, but with no order sensitivity and no deduping option.
 * 
 * @author Eyal Schneider
 */
public class AsyncUnionPipe <T> extends AsyncPipe<T> {
  private static final Logger logger = LoggerFactory.getLogger(AsyncUnionPipe.class);
  
  private final Collection<AsyncPipe<T>> inputPipes;
  private final AtomicInteger doneSuccessfulCounter = new AtomicInteger();
  
  /**
   * Constructor
   * 
   * @param inputPipes The input pipes to unify
   */
  public AsyncUnionPipe(Collection<AsyncPipe<T>> inputPipes) {
    this.inputPipes = inputPipes;
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    for (AsyncPipe<T> inputPipe : inputPipes) {
      inputPipe.setListener(new Listener(inputPipe));
      inputPipe.start();
    }
  }

  @Override
  public float getProgress() {
    return PipeUtils.getAverageProgress(inputPipes);
  }

  @Override
  public void close() throws IOException {
    super.close();
    PipeUtils.close(inputPipes);
  }
  
  private class Listener implements AsyncPipeListener<T> {
    private final AsyncPipe<T> inputPipe;

    public Listener(AsyncPipe<T> inputPipe) {
      this.inputPipe = inputPipe;
    }

    @Override
    public void next(T item) throws PipeException, InterruptedException {
       notifyNext(item);
    }

    @Override
    public void done() throws InterruptedException {
      if (doneSuccessfulCounter.incrementAndGet() == inputPipes.size()) {
        notifyDone();
      }
    }

    @Override
    public void error(PipeException e) throws InterruptedException {
      // First we make sure we don't get any further notifications
      for (AsyncPipe<?> pipe : inputPipes) {
        if (pipe != inputPipe) { // We don't want to send a close() command to the pipe reporting the error - this can cause a deadlock
                                 // since it should wait for all notifications originating from it to end first
          try {
            pipe.close();
          } catch (IOException e2) {
            logger.error("Unable to close pipe. Abnormal termination with possible notifications during/after the error notification of the async pipe.", e);
          }
        }
      }
      // Now we can safely pass on the error notification, knowing that no additional upstream originated notifications are expected
      notifyError(e);
    }
  }
}
