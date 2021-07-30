package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import java.io.IOException;
import java.util.Comparator;

/**
 * An intermediate pipe that validates the order of the consumed elements by a comparator
 *
 * @param <T> The items data type
 *
 * @author Eyal Rubichi
 */
public class OrderValidationPipe<T> implements Pipe<T> {

  private final Pipe<T> input;
  private final Comparator<T> comparator;
  private T next;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param comparator The comparator defining order relation on type T
   */
  public OrderValidationPipe(Pipe<T> input, Comparator<T> comparator) {
    this.input = input;
    this.comparator = comparator;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T currentItem = next;
    next = input.next();
    if (next != null && comparator.compare(currentItem, next) > 0) {
      throw new ValidationPipeException("Encountered two non ordered elements");
    }
    return currentItem;
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    prepareNext();
  }

  private void prepareNext() throws PipeException, InterruptedException {
    next = input.next();
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
