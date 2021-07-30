package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.exceptions.OutOfOrderPipeException;
import org.pipecraft.pipes.sync.inter.SortedIntersectionPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * Tests the pipe performing intersection of sorted pipes
 * 
 * @author Eyal Schneider
 */
public class SortedIntersectionPipeTest {

  @Test
  public void testPartialMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(1, 2, 3, 4, 5, 6);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 4, 6, 8, 10);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertEquals(Integer.valueOf(6), p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testPrefixMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4, 5, 6, 7);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 3);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testSuffixMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4, 5, 6, 7);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(6, 7);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertEquals(Integer.valueOf(6), p.next());
      assertEquals(Integer.valueOf(7), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testFullMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 3, 4);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testNoMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(1, 5, 6);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testOneInputPipeEmpty() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(1, 5, 6);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, EmptyPipe.instance())) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testSingleInput() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testNoInput() throws Exception {
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo)) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testOutOfOrder() throws Exception {
    assertThrows(OutOfOrderPipeException.class, () -> {
      CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
      CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(1, 3, 2);
      try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
        p.start();
        while (p.peek() != null) {
          p.next();
        }
      }
    });
  }
  
  @Test
  public void testRepetitions() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 2, 3, 4, 4, 4, 5);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 3, 3 ,4, 4 , 6, 6, 6);
    try (SortedIntersectionPipe<Integer> p = new SortedIntersectionPipe<>(Integer::compareTo, p1, p2)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }
}
