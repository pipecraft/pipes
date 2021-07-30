package org.pipecraft.pipes.sync.source;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;
import java.util.Iterator;


/**
 * A pipe based on an iterator.
 *
 * @param <T> the type of the output items
 *
 * @author Michal Rockban
 */
public class IteratorReaderPipe<T> implements Pipe<T> {

  private final Iterator<T> iterator;
  private T next;
  private Integer totalSize;  //optional
  private int pos;

  /**
   * Constructor
   *
   * @param iterator The iterator of the collection to be produced by this pipe
   */
  public IteratorReaderPipe(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  /**
   * Constructor
   *
   * @param iterator The iterator of the collection to be produced by this pipe
   * @param totalSize The iterator's collection size (optional). Used to compute progress.
   */
  public IteratorReaderPipe(Iterator<T> iterator, Integer totalSize) {
    this(iterator);
    this.totalSize = totalSize;
  }


  @Override
  public void close() throws IOException {
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    if (next == null) {
      return null;
    }
    T current = next;
    prepareNext();
    pos++;
    return current;
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    prepareNext();
  }

  private void prepareNext() {
    this.next = iterator.hasNext() ? iterator.next() : null;
  }

  @Override
  public float getProgress() {
    if (totalSize != null) {
      return pos / (float) totalSize;
    }
    // Basic implementation, due to missing information about the collection's size.
    return next == null ? 1 : 0;
  }
}
