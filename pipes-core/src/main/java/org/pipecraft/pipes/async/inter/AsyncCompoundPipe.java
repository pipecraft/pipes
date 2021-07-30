package org.pipecraft.pipes.async.inter;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

import java.io.IOException;

/**
 * A pipe encapsulating a pipeline.
 * Subclasses should implement createPipeline() method.
 *
 * @author Zacharya Haitin
 */
public abstract class AsyncCompoundPipe<T> extends AsyncPipe<T> {
  private AsyncPipe<T> innerPipe;

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      innerPipe = createPipeline();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }

    innerPipe.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(T item) throws InterruptedException {
        notifyNext(item);
      }

      @Override
      public void done() throws InterruptedException {
        notifyDone();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }
    });

    innerPipe.start();
  }

  /**
   * @return A new pipeline to represent the logic of this pipe
   * @throws IOException In case of IO error while building the internal pipeline
   */
  protected abstract AsyncPipe<T> createPipeline() throws IOException;

  @Override
  public void close() throws IOException {
    super.close();
    if (innerPipe != null) {
      innerPipe.close();
    }
  }

  @Override
  public float getProgress() {
    return innerPipe.getProgress();
  }
}

