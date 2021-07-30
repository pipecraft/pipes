package org.pipecraft.pipes.async.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.exceptions.TimeoutPipeException;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * Tests {@link AsyncTimeoutPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncTimeoutPipeTest {

//  @Test
  public void testSuccess() throws Exception {
    Collection<Integer> items = Collections.synchronizedCollection(new ArrayList<>());
    try (
        DummyAsyncPipe gen = new DummyAsyncPipe(0, 3, true);
        AsyncTimeoutPipe<Integer> timeoutP = new AsyncTimeoutPipe<>(gen, Duration.ofSeconds(5), Executors.newScheduledThreadPool(1));
        AsyncCallbackPipe<Integer> callbackP = new AsyncCallbackPipe<>(timeoutP, items::add);
        TerminalPipe tp = new AsyncConsumerPipe<>(callbackP);
        ) {
      tp.start();
      assertEquals(Sets.newHashSet(0, 1, 2), new HashSet<>(items));
    }
  }
  
  @Test
  public void testFailure() throws Exception {
    assertThrows(TimeoutPipeException.class, () -> {
        try (
            DummyAsyncPipe gen = new DummyAsyncPipe(0, Integer.MAX_VALUE, true); // Infinite stream
            AsyncTimeoutPipe<Integer> timeoutP = new AsyncTimeoutPipe<>(gen, Duration.ofMillis(5), Executors.newScheduledThreadPool(1)); 
            TerminalPipe tp = new AsyncConsumerPipe<>(timeoutP);
            ) {
          tp.start();
        }
      });
  }
}
