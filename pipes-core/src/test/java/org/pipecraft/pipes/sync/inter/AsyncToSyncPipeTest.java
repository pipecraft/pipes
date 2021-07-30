package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * Tests {@link AsyncToSyncPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncToSyncPipeTest {

  @Test
  public void testEmpty() throws Exception {
    try (DummyAsyncPipe ap = new DummyAsyncPipe(0, 0, true);
        AsyncToSyncPipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10, () -> new Integer(0))) {
      a2q.start();
      assertNull(a2q.next());
      assertEquals(1.0, a2q.getProgress(), 0.0001);
    }
  }

  @Test
  public void testSuccessfulEnd() throws Exception {
    try (DummyAsyncPipe ap = new DummyAsyncPipe(0, 100, true);
        AsyncToSyncPipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10, () -> new Integer(0))) {
      a2q.start();
      
      Integer next;
      Set<Integer> all = new HashSet<>();
      while ((next = a2q.next()) != null) {
        all.add(next);
      }
      assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), all);
      assertEquals(1.0, a2q.getProgress(), 0.0001);
    }
  }

  @Test
  public void testUnsuccessfulEnd() throws Exception {
    try (DummyAsyncPipe ap = new DummyAsyncPipe(0, 100, false);
        AsyncToSyncPipe<Integer> a2q = new AsyncToSyncPipe<Integer>(ap, 10, () -> new Integer(0))) {
      a2q.start();
      
      Integer next;
      Set<Integer> all = new HashSet<>();
      try {
        while ((next = a2q.next()) != null) {
          all.add(next);
        }
      } catch (ValidationPipeException e) {
        assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), all);  
        return;
      }
      fail("Expected an exception");
    }
  }
}
