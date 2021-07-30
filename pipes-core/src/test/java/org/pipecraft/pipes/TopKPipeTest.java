package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.TopKPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * Tests {@link TopKPipe}, which returns the top k items of an input pipe according to a given ordering relation
 * 
 * @author Eyal Schneider
 */
public class TopKPipeTest {
  @Test
  public void testEmpty() throws Exception {
    try (
        TopKPipe<Integer> p = new TopKPipe<>(EmptyPipe.instance(), 3, Integer::compareTo);
     ) {
      p.start();
      assertNull(p.next());
    }
  }
  
  @Test
  public void testLessThanK() throws Exception {
    try (
        CollectionReaderPipe<Integer> p0 = new CollectionReaderPipe<>(9, 3, 7, 7);
        TopKPipe<Integer> p = new TopKPipe<>(p0, 5, Integer::compareTo);
     ) {
      p.start();
      assertEquals(Integer.valueOf(9), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testMoreThanK() throws Exception {
    try (
        CollectionReaderPipe<Integer> p0 = new CollectionReaderPipe<>(9, 3, 3, 7, 3, 7, 2, 6, 4, 1);
        TopKPipe<Integer> p = new TopKPipe<>(p0, 3, Integer::compareTo);
     ) {
      p.start();
      assertEquals(Integer.valueOf(9), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertNull(p.next());
    }
  }
}
