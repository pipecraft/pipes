package org.pipecraft.pipes.async.inter;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.sync.source.ErrorPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

/**
 * Tests {@link SyncToAsyncPipe}
 *
 * @author Eyal Schneider
 */
public class SyncToAsyncPipeTest {

  @Test
  public void testSingleInputPipe() throws Exception {
    List<Long> producedItems = Collections.synchronizedList(new ArrayList<>());
    try (Pipe<Long> p0 = new SeqGenPipe<Long>(Functions.identity(), 10);
         AsyncPipe<Long> p1 = SyncToAsyncPipe.fromPipe(p0);
         TerminalPipe tp = new AsyncConsumerPipe<>(p1, producedItems::add)) {
      tp.start();
      assertEquals(10, producedItems.size());
      assertEquals(Sets.newHashSet(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), new HashSet<>(producedItems));
    }
  }

  @Test
  @Timeout(40)
  public void testStoppingByClose() throws Exception {
    assertTimeout(Duration.ofSeconds(30), () -> {
      try (Pipe<Long> p01 = new SeqGenPipe<Long>(Functions.identity()); // Infinite stream
           Pipe<Long> p02 = new SeqGenPipe<Long>(Functions.identity()); // Infinite stream
           AsyncPipe<Long> p1 = SyncToAsyncPipe.fromPipes(Lists.newArrayList(p01, p02), 2);
           TerminalPipe tp = new AsyncConsumerPipe<>(p1)) {

        new Thread(() -> { // Send the close signal from another thread (Async pipes are supposed to support this)
          try {
            Thread.sleep(300);
            tp.close();
          } catch (IOException | InterruptedException e) {
          }
        }).start();

        tp.start();
      }
    });
  }

  @Test
  public void testThreePipesTwoThreads() throws Exception {
    List<Long> producedItems = Collections.synchronizedList(new ArrayList<>());
    try (Pipe<Long> p01 = new SeqGenPipe<Long>(i -> 3 * i, 3); // 0, 3, 6
         Pipe<Long> p02 = new SeqGenPipe<Long>(i -> 3 * i + 1, 3); // 1, 4, 7
         Pipe<Long> p03 = new SeqGenPipe<Long>(i -> 3 * i + 2, 3); // 2, 5, 8
         AsyncPipe<Long> p1 = SyncToAsyncPipe.fromPipes(Lists.newArrayList(p01, p02, p03), 2);
         TerminalPipe tp = new AsyncConsumerPipe<>(p1, producedItems::add)) {
      tp.start();
      assertEquals(9, producedItems.size());
      assertEquals(Sets.newHashSet(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), new HashSet<>(producedItems));
    }
  }

  @Test
  public void testTwoPipesThreeThreads() throws Exception {
    List<Long> producedItems = Collections.synchronizedList(new ArrayList<>());
    try (Pipe<Long> p01 = new SeqGenPipe<Long>(i -> 2 * i, 3); // 0, 2, 4
         Pipe<Long> p02 = new SeqGenPipe<Long>(i -> 2 * i + 1, 3); // 1, 3, 5
         AsyncPipe<Long> p1 = SyncToAsyncPipe.fromPipes(Lists.newArrayList(p01, p02), 3);
         TerminalPipe tp = new AsyncConsumerPipe<>(p1, producedItems::add)) {
      tp.start();
      assertEquals(6, producedItems.size());
      assertEquals(Sets.newHashSet(0L, 1L, 2L, 3L, 4L, 5L), new HashSet<>(producedItems));
    }
  }

  @Test
  public void testStoppingByError() throws Exception {
    PipeException e = new ValidationPipeException("Test");
    try (Pipe<Long> p0 = new SeqGenPipe<Long>(i -> i, 100);
         Pipe<Long> p1 = ConcatPipe.fromPipes(p0, new ErrorPipe<Long>(e));
         AsyncPipe<Long> p2 = SyncToAsyncPipe.fromPipes(Collections.singletonList(p1), 2);
         TerminalPipe tp = new AsyncConsumerPipe<>(p2)) {
      try {
        tp.start();
      } catch (Exception e1) {
        assertEquals(e, e1);
      }
    }
  }

  @Test
  public void testTwoPipesOneThread() throws Exception {
    List<Long> producedItems = Collections.synchronizedList(new ArrayList<>());
    try (Pipe<Long> p01 = new SeqGenPipe<Long>(i -> 2 * i + 1, 5); // 1, 3, 5, 7, 9
         Pipe<Long> p02 = new SeqGenPipe<Long>(i -> 2 * i, 5); // 0, 2, 4, 6, 8
         AsyncPipe<Long> p1 = SyncToAsyncPipe.fromPipes(Lists.newArrayList(p01, p02), 1);
         TerminalPipe tp = new AsyncConsumerPipe<>(p1, producedItems::add)) {
      tp.start();
      assertEquals(10, producedItems.size());
      assertEquals(Sets.newHashSet(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), new HashSet<>(producedItems));
    }
  }

}
