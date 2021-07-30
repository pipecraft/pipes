package org.pipecraft.pipes.sync.inter;

import java.util.function.Predicate;

import org.pipecraft.pipes.sync.Pipe;

/**
 * A pipe filtering an input pipe by some predicate on the pipe items.
 * 
 * @author Eyal Schneider
 */
public class FilterPipe<T> extends FilterBasePipe<T> {
  private final Predicate<? super T> predicate;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   */
  public FilterPipe(Pipe<T> input, Predicate<? super T> predicate) {
    super(input);
    this.predicate = predicate;
  }

  @Override
  protected boolean shouldSelect(T item) {
    return predicate.test(item);
  }
}
