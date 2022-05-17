package org.pipecraft.pipes.async.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.async.inter.AsyncCallbackPipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;

/**
 * Tests {@link AsyncSeqGenPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncSeqGenPipeTest {

  @Test
  public void testEmpty() throws Exception {
    List<String> items = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncSeqGenPipe<String> gen = new AsyncSeqGenPipe<>(0, String::valueOf, 10); // 10 threads producing the items "0", "1",...,"999"
        AsyncCallbackPipe<String> callback = new AsyncCallbackPipe<>(gen, items::add);
        AsyncConsumerPipe<String> consumer = new AsyncConsumerPipe<>(callback)) {
      consumer.start();
      assertEquals(0, items.size());
    }
  }

  @Test
  public void testCompletion() throws Exception {
    List<String> items = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncSeqGenPipe<String> gen = new AsyncSeqGenPipe<>(1000, String::valueOf, 10); // 10 threads producing the items "0", "1",...,"999"
        AsyncCallbackPipe<String> callback = new AsyncCallbackPipe<>(gen, items::add);
        AsyncConsumerPipe<String> consumer = new AsyncConsumerPipe<>(callback)) {
      consumer.start();
      assertEquals(1000, items.size());
      assertEquals(IntStream.range(0, 1000).boxed().map(String::valueOf).collect(Collectors.toSet()), new HashSet<>(items));
      assertEquals(1.0, consumer.getProgress(), 0.001);
    }
  }
  
  @Test
  public void testInterruptingClose() {
    assertTimeout(Duration.ofSeconds(10), () -> {
      try (
          AsyncSeqGenPipe<String> gen = new AsyncSeqGenPipe<>(Long.MAX_VALUE, String::valueOf, 10); // 10 threads practically trying to produce infinite number of items
          AsyncConsumerPipe<String> consumer = new AsyncConsumerPipe<>(gen)) {
        AtomicReference<Exception> err = new AtomicReference<>();
        new Thread(() -> { 
          try {
            consumer.start();
          } catch (PipeException | InterruptedException e) {
            err.set(e);
          }
        }).start();
        
        Thread.sleep(300);
        assertNull(err.get());
      }
    });
  }

}
