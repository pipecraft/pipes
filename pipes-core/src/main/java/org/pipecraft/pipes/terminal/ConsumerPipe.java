package org.pipecraft.pipes.terminal;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.concurrent.FailableConsumer;

/**
 * A terminal pipe that consumes all contents of the input pipe in a synchronous manner.
 * That is, calling start() will loop in over all outputs of the input pipe, performing the given action
 * on each item.
 * 
 * @param <T> The input items' data type
 *
 * @author Eyal Schneider
 */
public class ConsumerPipe<T> extends TerminalPipe {
  private final Pipe<T> input;
  private final FailableConsumer<? super T, PipeException> action;
  private final Runnable terminationAction;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param itemAction The action to perform on all items. Legitimate errors should be wrapped by PipeException.
   * @param terminationAction An action to perform once all input items have been consumed. Runs once, upon a successful iteration termination only. 
   */
  public ConsumerPipe(Pipe<T> input, FailableConsumer<? super T, PipeException> itemAction, Runnable terminationAction) {
    this.input = input;
    this.action = itemAction;
    this.terminationAction = terminationAction;
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param itemAction The action to perform on all items. Legitimate errors should be wrapped by PipeException. 
   */
  public ConsumerPipe(Pipe<T> input, FailableConsumer<? super T, PipeException> itemAction) {
    this(input, itemAction, () -> {});
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param terminationAction An action to perform once all input items have been consumed. Runs once, upon a successful iteration termination only. 
   */
  public ConsumerPipe(Pipe<T> input, Runnable terminationAction) {
    this(input, v -> {}, terminationAction);
  }

  /**
   * Constructor
   * 
   * Builds a no-op consumer
   * @param input The input pipe
   */
  public ConsumerPipe(Pipe<T> input) {
    this(input, v -> {}, () -> {});
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    T item;
    while ((item = input.next()) != null) {
      action.accept(item);
    }
    terminationAction.run();
  }
}
