package org.pipecraft.pipes.async.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * Tests {@link AsyncHeadPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncHeadPipeTest {

  @Test
  public void testTruncation() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    try (
        AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 10, true);
        AsyncPipe<Integer> headP = new AsyncHeadPipe<>(gen, 3); // Keeps only 3 numbers
        AsyncPipe<Integer> callbackP = new AsyncCallbackPipe<>(headP, v -> counter.incrementAndGet());
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP)) {
      tp.start();
      assertEquals(3, counter.get());
    }
  }

  @Test
  public void testNoTruncation() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    try (
        AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 10, true);
        AsyncPipe<Integer> headP = new AsyncHeadPipe<>(gen, 11); // There are only 10 available items so they are all returned
        AsyncPipe<Integer> callbackP = new AsyncCallbackPipe<>(headP, v -> counter.incrementAndGet());
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP)) {
      tp.start();
      assertEquals(10, counter.get());
    }
  }

  @Test
  public void testFailureThatShouldBeSupressed() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    try (
        AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 1000, false); // fails after producing 1000 items
        AsyncPipe<Integer> headP = new AsyncHeadPipe<>(gen, 3); // Take only 3 items
        AsyncPipe<Integer> callbackP = new AsyncCallbackPipe<>(headP, v -> counter.incrementAndGet());
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP)) {
      tp.start();
      assertEquals(3, counter.get());
    }
  }
  
  @Test
  public void testFailure() {
    assertThrows(ValidationPipeException.class, () -> {
        try (
            DummyAsyncPipe gen = new DummyAsyncPipe(0, 10, false);
            AsyncHeadPipe<Integer> headP = new AsyncHeadPipe<>(gen, 13);
            TerminalPipe tp = new AsyncConsumerPipe<>(headP)) {
          tp.start();
        }
      });
  }

}
