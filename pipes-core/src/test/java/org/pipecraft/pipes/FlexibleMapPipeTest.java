package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.inter.FlexibleMapPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;

/**
 * Tests {@link FlexibleMapPipe}
 * 
 * @author Eyal Schneider
 */
public class FlexibleMapPipeTest {

  @Test
  public void testEmptyInput() throws Exception {
    try(Pipe<Integer> p = new FlexibleMapPipe<Integer, Integer>(EmptyPipe.instance(), v -> new CollectionReaderPipe<>(Collections.emptyList()))) {
      p.start();
      assertNull(p.next());
    }
  }
  
  @Test
  public void testMapOneToZero() throws Exception {
    try(
        Pipe<Integer> p0 = new CollectionReaderPipe<>(Arrays.asList(1, 2, 3, 4, 5));
        Pipe<Integer> p = new FlexibleMapPipe<Integer, Integer>(p0, v -> EmptyPipe.instance())) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testMapOneToOne() throws Exception {
    try(
        Pipe<Integer> p0 = new CollectionReaderPipe<>(Arrays.asList(1, 2, 3));
        Pipe<Integer> p = new FlexibleMapPipe<Integer, Integer>(p0, v -> new CollectionReaderPipe<>(Collections.singletonList(2 * v)))) {
      p.start();
      assertEquals(2, p.next().intValue());
      assertEquals(4, p.next().intValue());
      assertEquals(6, p.next().intValue());
      assertNull(p.next());
    }    
  }

  @Test
  public void testExpand() throws Exception {
    try(
        Pipe<Integer> p0 = new CollectionReaderPipe<>(Arrays.asList(1, 2, 3));
        Pipe<Integer> p = new FlexibleMapPipe<Integer, Integer>(p0, v -> new CollectionReaderPipe<>(Arrays.asList(2 * v, 2 * v + 1)))) {
      p.start();
      assertEquals(2, p.next().intValue());
      assertEquals(3, p.next().intValue());
      assertEquals(4, p.next().intValue());
      assertEquals(5, p.next().intValue());
      assertEquals(6, p.next().intValue());
      assertEquals(7, p.next().intValue());
      assertNull(p.next());
    }        
  }

  @Test
  public void testGeneralMap() throws Exception {
    try(
        Pipe<Integer> p0 = new CollectionReaderPipe<>(Arrays.asList(1, 2, 3, 4, 5));
        Pipe<Integer> p = new FlexibleMapPipe<Integer, Integer>(p0, v -> {
          return v % 2 == 0 ? EmptyPipe.instance() : new CollectionReaderPipe<>(Arrays.asList(2 * v, 2 * v + 1));
        })) {
      p.start();
      assertEquals(2, p.next().intValue());
      assertEquals(3, p.next().intValue());
      assertEquals(6, p.next().intValue());
      assertEquals(7, p.next().intValue());
      assertEquals(10, p.next().intValue());
      assertEquals(11, p.next().intValue());
      assertNull(p.next());
    }            
  }
}
