package org.pipecraft.pipes.async.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * Tests {@link AsyncFlexibleMapPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncFlexibleMapPipeTest {

  @Test
  public void testEmptyInput() throws Exception {
    AtomicInteger itemsTraversed = new AtomicInteger();
    try(
        AsyncPipe<Integer> prod = new AsyncEmptyPipe<>();
        AsyncPipe<Integer> fmp = new AsyncFlexibleMapPipe<Integer, Integer>(prod, v -> new CollectionReaderPipe<>(1, 2, 3));
        TerminalPipe tp = new AsyncConsumerPipe<>(fmp, v -> itemsTraversed.incrementAndGet());
        ) {
      tp.start();
    }
    assertEquals(0, itemsTraversed.get());
  }
  
  @Test
  public void testMapOneToOneOrZero() throws Exception {
    Set<Integer> itemsTraversed = Collections.synchronizedSet(new HashSet<>());
    try(
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(5, v -> (int)(v + 1), 2);
        AsyncPipe<Integer> fmp = new AsyncFlexibleMapPipe<>(prod, v -> {
          if (v % 2 == 0) { // Filter out even values
            return EmptyPipe.instance();
          } 
          return new CollectionReaderPipe<>(v);
        });
        TerminalPipe tp = new AsyncConsumerPipe<>(fmp, v -> itemsTraversed.add(v))) {
      tp.start();
    }
    assertEquals(Sets.newHashSet(1, 3, 5), itemsTraversed);
  }

  @Test
  public void testMapOneToOne() throws Exception {
    Set<Integer> itemsTraversed = Collections.synchronizedSet(new HashSet<>());
    try(
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(5, v -> (int)(v + 1), 2);
        AsyncPipe<Integer> fmp = new AsyncFlexibleMapPipe<>(prod, v -> {
          return new CollectionReaderPipe<>(2 * v);
        });
        TerminalPipe tp = new AsyncConsumerPipe<>(fmp, v -> itemsTraversed.add(v))) {
      tp.start();
    }
    assertEquals(Sets.newHashSet(2, 4, 6, 8, 10), itemsTraversed);
  }

  @Test
  public void testExpand() throws Exception {
    Set<Integer> itemsTraversed = Collections.synchronizedSet(new HashSet<>());
    try(
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(5, v -> (int)(v + 1), 2);
        AsyncPipe<Integer> fmp = new AsyncFlexibleMapPipe<>(prod, v -> {
          return new CollectionReaderPipe<>(2 * v, 2 * v - 1);
        });
        TerminalPipe tp = new AsyncConsumerPipe<>(fmp, v -> itemsTraversed.add(v))) {
      tp.start();
    }
    assertEquals(Sets.newHashSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), itemsTraversed);
  }
}
