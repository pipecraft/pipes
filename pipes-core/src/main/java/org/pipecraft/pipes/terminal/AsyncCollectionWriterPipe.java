package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal async pipe writing all contents from an input pipe into a given collection.
 * 
 * Thread safety:
 * It's the responsibility of the caller to use a thread safe collection!
 * 
 * @author Eyal Schneider
 */
public class AsyncCollectionWriterPipe<T> extends TerminalPipe {
  private final AsyncPipe<T> input;
  private final Collection<T> outputCollection;
  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private PipeException exception;
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param outputCollection The output collection to write items to. Must be thread safe!
   */
  public AsyncCollectionWriterPipe(AsyncPipe<T> input, Collection<T> outputCollection) {
    this.input = input;
    this.outputCollection = outputCollection;
    
    input.setListener(new AsyncPipeListener<T>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        outputCollection.add(item);
      }

      @Override
      public void done() throws InterruptedException {
        terminationLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        exception = e;
        terminationLatch.countDown();
      }
    });
  }

  /**
   * Constructor
   * 
   * Collects items into a thread safe {@link List} created internally.
   * @param input The input pipe
   */
  public AsyncCollectionWriterPipe(AsyncPipe<T> input) {
    this(input, Collections.synchronizedList(new ArrayList<>()));
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    terminationLatch.await();
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * @return The collection given in the constructor
   */
  public Collection<T> getItems() {
    return outputCollection;
  }
}
