package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe writing all contents from an input pipe into a given collection.
 * 
 * Thread safety:
 * It's the responsibility of the caller to make sure access to the output collection is thread
 * safe. For example, if the collection itself is thread safe, or if a single thread creates, runs and queries the results (using getItems()) then
 * there's no concurrency problem.
 * 
 * @author Eyal Schneider
 */
public class CollectionWriterPipe<T> extends TerminalPipe {
  private final Pipe<T> input;
  private final Collection<T> outputCollection;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param outputCollection The output collection to write items to.
   */
  public CollectionWriterPipe(Pipe<T> input, Collection<T> outputCollection) {
    this.input = input;
    this.outputCollection = outputCollection;
  }

  /**
   * Constructor
   * 
   * Collects items into an {@link ArrayList}
   * @param input The input pipe
   */
  public CollectionWriterPipe(Pipe<T> input) {
    this(input, new ArrayList<>());
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    T next;
    while ((next = input.next()) != null) {
      outputCollection.add(next);
    }
  }

  /**
   * @return The collection given in the constructor
   */
  public Collection<T> getItems() {
    return outputCollection;
  }
}
