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
 * Tests {@link AsyncMapPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncMapPipeTest {

  @Test
  public void testSuccess() throws Exception {
    Collection<String> items = Collections.synchronizedCollection(new ArrayList<>());
    try (
        AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 3, true);
        AsyncPipe<String> mapP = new AsyncMapPipe<>(gen, v -> "_" + v.toString() + "_");
        AsyncPipe<String> callbackP = new AsyncCallbackPipe<>(mapP, items::add);
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP)) {
      tp.start();
      assertEquals(Sets.newHashSet("_0_", "_1_", "_2_"), new HashSet<>(items));
    }
  }
  
  @Test
  public void testFailure() {
    assertThrows(ValidationPipeException.class, () -> {
        try (
            AsyncPipe<Integer> gen = new DummyAsyncPipe(0, 3, false);
            AsyncPipe<String> mapP = new AsyncMapPipe<>(gen, v -> "_" + v.toString() + "_");
            TerminalPipe tp = new AsyncConsumerPipe<>(mapP)) {
          tp.start();
        }
      });
  }
}
