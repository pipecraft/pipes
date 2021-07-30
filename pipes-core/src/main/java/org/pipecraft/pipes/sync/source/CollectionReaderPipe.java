package org.pipecraft.pipes.sync.source;

import java.util.Collection;

import com.google.common.collect.Iterators;

/**
 * A pipe based on a given collection of items.
 * Produces items according to their iteration order in the given collection.
 * 
 * @author Eyal Schneider
 */
public class CollectionReaderPipe<T> extends IteratorReaderPipe<T> {

  /**
   * Constructor
   * 
   * @param items The items to be produced by this pipe
   */
  public CollectionReaderPipe(Collection<T> items) {
    super(items.iterator(), items.size());
  }

  /**
   * Constructor
   * 
   * @param items The items to be produced by this pipe, in the required order
   */
  @SafeVarargs
  public CollectionReaderPipe(T ... items) {
    super(Iterators.forArray(items), items.length);
  }
}
