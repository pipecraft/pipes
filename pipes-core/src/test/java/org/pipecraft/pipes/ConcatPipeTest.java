package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * A test for the {@link ConcatPipe}, which concats pipe contents
 * 
 * @author Eyal Schneider
 */
public class ConcatPipeTest {
  @Test
  public void testNoInputPipes() throws Exception {
    try (ConcatPipe<String> p = new ConcatPipe<>(Collections.emptyList())) {
      p.start();
      assertNull(p.next());
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testMultipleInputs() throws Exception {
    
    try (
        CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>();
        CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(1, 2, 3, 4);
        CollectionReaderPipe<Integer> p3 = new CollectionReaderPipe<>(4, 5);
        CollectionReaderPipe<Integer> p4 = new CollectionReaderPipe<>(6);
        CollectionReaderPipe<Integer> p5 = new CollectionReaderPipe<>();
        CollectionReaderPipe<Integer> p6 = new CollectionReaderPipe<>(7, 8);
        CollectionReaderPipe<Integer> p7 = new CollectionReaderPipe<>();
        ConcatPipe<Integer> p = new ConcatPipe<>(() -> p1, () -> p2, () -> p3, () -> p4, () -> p5, () -> p6, () -> p7)) {
      p.start();
      assertEquals(Integer.valueOf(1), p.next());
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertEquals(Integer.valueOf(5), p.next());
      assertEquals(Integer.valueOf(6), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertEquals(Integer.valueOf(8), p.next());
      assertNull(p.next());
    }
  }

}