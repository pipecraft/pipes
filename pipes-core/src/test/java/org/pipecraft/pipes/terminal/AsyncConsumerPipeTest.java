package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * Tests {@link AsyncConsumerPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncConsumerPipeTest {
  
  @Test
  public void testEmpty() throws Exception {
    Set<Integer> itemsReported = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger terminationActionCount = new AtomicInteger();
    try (
        AsyncPipe<Integer> prod = new AsyncEmptyPipe<>();
        TerminalPipe consumer = new AsyncConsumerPipe<>(prod, itemsReported::add, terminationActionCount::incrementAndGet)) {
      consumer.start();
    }
    
    assertEquals(0, itemsReported.size());
    assertEquals(1, terminationActionCount.get());
  }

  @Test
  public void testItemAndTerminationNonEmpty() throws Exception {
    Set<Integer> itemsReported = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger terminationActionCount = new AtomicInteger();
    try (
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(100, Long::intValue, 2);
        TerminalPipe consumer = new AsyncConsumerPipe<>(prod, itemsReported::add, terminationActionCount::incrementAndGet)) {
      consumer.start();
    }
    
    assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), itemsReported);
    assertEquals(1, terminationActionCount.get());
  }

  @Test
  public void testItemActionError() {
    Set<Integer> itemsReported = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger terminationActionCount = new AtomicInteger();
    
    assertThrows(ValidationPipeException.class, () -> {
      try (
          AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(1000, Long::intValue, 2);
          TerminalPipe consumer = new AsyncConsumerPipe<>(prod, item -> {
            itemsReported.add(item);
            if (itemsReported.size() > 500) {
              throw new ValidationPipeException("Test");
            }
          }
          , terminationActionCount::incrementAndGet)) {
        consumer.start();
      }
    });
    
    assertEquals(0, terminationActionCount.get()); // Termination action not expected to be invoked!
  }
}
