package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.Pipe;

/**
 * Tests {@link AsyncToSyncPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class AsyncToSyncPipeTest {

  @Test
  public void testEmpty() throws Exception {
    try (AsyncPipe<Integer> ap = new DummyAsyncPipe(0, 0, true);
        Pipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10)) {
      a2q.start();
      assertNull(a2q.next());
      assertEquals(1.0, a2q.getProgress(), 0.0001);
    }
  }

  @Test
  public void testSuccessfulEnd() throws Exception {
    try (AsyncPipe<Integer> ap = new DummyAsyncPipe(0, 100, true);
        Pipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10)) {
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
    try (AsyncPipe<Integer> ap = new DummyAsyncPipe(0, 100, false);
        Pipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10)) {
      a2q.start();
      
      Integer next;
      Set<Integer> all = new HashSet<>();
      try {
        while ((next = a2q.next()) != null) {
          all.add(next);
        }
      } catch (PipeException e) {
        assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), all);  
        return;
      }
      fail("Expected an exception");
    }
  }

  @Test
  public void testPeek() throws Exception {
    try (AsyncPipe<Integer> ap = new DummyAsyncPipe(1, 1, true);
        Pipe<Integer> a2q = new AsyncToSyncPipe<>(ap, 10)) {
      a2q.start();

      assertEquals(1, a2q.peek());
      assertEquals(1, a2q.next());
      assertNull(a2q.peek());
      assertNull(a2q.next());
    }
  }

}
