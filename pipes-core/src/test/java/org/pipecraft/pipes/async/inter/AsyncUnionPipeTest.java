package org.pipecraft.pipes.async.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;

/**
 * Tests {@link AsyncUnionPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncUnionPipeTest {
  @Test
  public void testSuccessful() throws Exception {
    Collection<Integer> seenValues = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncPipe<Integer> p0 = new AsyncEmptyPipe<>();
        DummyAsyncPipe p1 = new DummyAsyncPipe(0, 50, true);
        DummyAsyncPipe p2 = new DummyAsyncPipe(50, 150, true);
        DummyAsyncPipe p3 = new DummyAsyncPipe(200, 100, true);
        AsyncUnionPipe<Integer> unionP = new AsyncUnionPipe<>(Lists.newArrayList(p1, p2, p3));
        AsyncCallbackPipe<Integer> callbackP = new AsyncCallbackPipe<>(unionP, seenValues::add);
        AsyncConsumerPipe<?> cp = new AsyncConsumerPipe<>(callbackP);
        ) {
      cp.start();
      assertEquals(IntStream.range(0, 300).boxed().collect(Collectors.toSet()), new HashSet<>(seenValues));
      assertEquals(1.0, unionP.getProgress(), 0.0001);
    }
  }
  
  @Test
  public void testFailureInOnePipe() throws Exception {
    Collection<Integer> seenValues = Collections.synchronizedList(new ArrayList<>());
    try (
        DummyAsyncPipe p1 = new DummyAsyncPipe(0, 50, false);
        DummyAsyncPipe p2 = new DummyAsyncPipe(50, 150, true);
        DummyAsyncPipe p3 = new DummyAsyncPipe(200, 100, true);
        AsyncUnionPipe<Integer> unionP = new AsyncUnionPipe<>(Lists.newArrayList(p1, p2, p3));
        AsyncCallbackPipe<Integer> callbackP = new AsyncCallbackPipe<>(unionP, seenValues::add);
        AsyncConsumerPipe<?> cp = new AsyncConsumerPipe<>(callbackP);
        ) {
      assertThrows(ValidationPipeException.class, () -> cp.start());
    }
  }
}
