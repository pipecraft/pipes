package org.pipecraft.pipes.async.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.FlexibleMapPipe;
import org.pipecraft.pipes.utils.PipeFactory;

/**
 * An async version of the flexible mapper pipe (See {@link FlexibleMapPipe}.
 * 
 * @param <S> The source item data type
 * @param <T> The target item data type
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncFlexibleMapPipe <S, T> extends AsyncPipe<T> {
  private final AsyncPipe<S> input;
  private final PipeFactory<S, ? extends T> factory;

  /**
   * constructor
   *
   * @param input The input pipe
   * @param factory A factory mapping an input item to a pipe of items of the target type
   */
  public AsyncFlexibleMapPipe(AsyncPipe<S> input, PipeFactory<S, ? extends T> factory) {
    this.input = input;
    this.factory = factory;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(S item) throws PipeException, InterruptedException {
        try {
          Pipe<? extends T> p = factory.get(item);
          p.start();
          T next;
          while ((next = p.next()) != null) {
            notifyNext(next);  
          }
        } catch (IOException e) {
          throw new IOPipeException(e);
        }
      }

      @Override
      public void done() throws InterruptedException {
        notifyDone();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        notifyError(e);
      }});
    input.start();
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }

  @Override
  public void close() throws IOException {
    super.close();
    input.close();
  }
}
