package org.pipecraft.pipes.async.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * Tests {@link AsyncFilterPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncFilterPipeTest {

  @Test
  public void testSuccess() throws Exception {
    Collection<Integer> items = Collections.synchronizedCollection(new ArrayList<>());
    try (
        AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 10, true);
        AsyncPipe<Integer> filterP = new AsyncFilterPipe<>(gen, v -> v % 2 == 0); // Keeps only even numbers
        AsyncPipe<Integer> callbackP = new AsyncCallbackPipe<>(filterP, items::add);
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP)) {
      tp.start();
      assertEquals(Sets.newHashSet(0, 2, 4, 6, 8), new HashSet<>(items));
    }
  }
  
  @Test
  public void testFailure() {
    assertThrows(ValidationPipeException.class, () -> {
        try (
            AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 10, false);
            AsyncPipe<Integer> filterP = new AsyncFilterPipe<>(gen, v -> v % 2 == 0); // Keeps only even numbers
            TerminalPipe tp = new AsyncConsumerPipe<>(filterP)) {
          tp.start();
        }
      });
  }
}
