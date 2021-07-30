package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.google.common.base.Functions;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import org.pipecraft.pipes.terminal.PercentilePipe;

/**
 * Tests {@link PercentilePipe}
 * 
 * @author Eyal Schneider
 */
public class PercentilePipeTest {

  @Test
  public void testIllegalPercentile() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> {
      try (
          PercentilePipe<Integer, Integer> p = new PercentilePipe<Integer, Integer>(EmptyPipe.instance(), 1.1, Functions.identity())) {      
      }
    });
  }

  @Test
  public void testEmpty() throws Exception { 
    try (
        PercentilePipe<Integer, Integer> p = new PercentilePipe<Integer, Integer>(EmptyPipe.instance(), 0.5, Functions.identity())) {
      p.start();
      assertNull(p.getPercentileValue());
    }
  }

  @Test
  public void testSingleItem() throws Exception {
    // In this test the inputs are string and we look for a percentile by string lengths. 
    for (int i = 0; i <= 10; i++) {
      try (
          CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("hello");
          PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 0.1 * i, s -> s.length());
          ) {
        p.start();
        assertEquals(Integer.valueOf(5), p.getPercentileValue()); // We have only one string of length 5 so it should be returned for all required percentiles 
      }
    }
  }

  @Test
  public void testSingleValueRepeated() throws Exception {
    // In this test the inputs are string and we look for a percentile by string lengths. 
    for (int i = 0; i <= 10; i++) {
      try (
          CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("fox", "dog", "cat");
          PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 0.1 * i, s -> s.length());
          ) {
        p.start();
        assertEquals(Integer.valueOf(3), p.getPercentileValue()); // All strings are of lenth 3, so it should be returned for all required percentiles 
      }
    }
  }

  @Test
  public void testMin() throws Exception {    
    // In this test the inputs are string and we look for a percentile by string lengths. 
    try (
        CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("hello", "Alex", "I", "miss", "you", "dude");
        PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 0.0, s -> s.length());
        ) {
      p.start();
      assertEquals(Integer.valueOf(1), p.getPercentileValue()); // 1 is the smallest string length 
    }
  }

  @Test
  public void testMax() throws Exception {    
    // In this test the inputs are string and we look for a percentile by string lengths. 
    try (
        CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("hello", "Alex", "I", "miss", "you", "dude");
        PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 1.0, s -> s.length());
        ) {
      p.start();
      assertEquals(Integer.valueOf(5), p.getPercentileValue()); // 5 is the largest string length 
    }
  }

  @Test
  public void testGeneral1() throws Exception {    
    // In this test the inputs are string and we look for a percentile by string lengths. 
    try (
        CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("hello", "Alex", "I", "miss", "you", "dude");
        PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 0.3333, s -> s.length());
        ) {
      p.start();
      assertEquals(Integer.valueOf(3), p.getPercentileValue()); // We have exactly 2 strings (out of 6) of length <=3, so this is the 0.333 percentile
    }
  }
  
  @Test
  public void testGeneral2() throws Exception {    
    // In this test the inputs are string and we look for a percentile by string lengths. 
    try (
        CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("hello", "Alex", "I", "miss", "you", "dude");
        PercentilePipe<String, Integer> p = new PercentilePipe<>(p0, 0.5, s -> s.length());
        ) {
      p.start();
      assertEquals(Integer.valueOf(4), p.getPercentileValue()); // We have 2 strings smaller than 4, and 1 larger than 4, therefore 4 is the 0.5 percentile
    }
  }

  @Test
  public void testSortedInts() throws Exception {    
    // In this test the inputs are string and we look for a percentile by string lengths. 
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i + 1, 100); // 1..100
        PercentilePipe<Long, Long> p = new PercentilePipe<>(p0, 0.25, Functions.identity());
        ) {
      p.start();
      assertEquals(Long.valueOf(25), p.getPercentileValue()); 
    }
  }

}
