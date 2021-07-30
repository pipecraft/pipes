package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.FilterPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * A test for the {@link FilterPipe}, which filters a given input pipe by a predicate on the items
 * 
 * @author Eyal Schneider
 */
public class FilterPipeTest {
  @Test
  public void testSelectAll() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (FilterPipe<Integer> p = new FilterPipe<>(p1, v -> true)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testSelectNone() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (FilterPipe<Integer> p = new FilterPipe<>(p1, v -> false)) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testSelectSome() throws Exception {
    final int n = 1000;
    SeqGenPipe<Integer> p1 = new SeqGenPipe<>(i -> (i < n) ? i.intValue() : null); // 0 - 999
    try (FilterPipe<Integer> p = new FilterPipe<>(p1, v -> v % 2 == 0)) { // Only even numbers
      p.start();
      int selectedCount = 0;
      while (p.next() != null) {
        selectedCount++;
      }
      assertEquals(n / 2, selectedCount);
    }
  }

}
