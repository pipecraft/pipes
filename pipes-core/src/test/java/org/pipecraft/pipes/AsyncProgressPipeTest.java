package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.inter.AsyncProgressPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * A test for {@link AsyncProgressPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncProgressPipeTest {
  @Test
  public void testProgress() throws Exception {
    List<Integer> progressLog = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncPipe<String> p1 = new AsyncSeqGenPipe<>(100, i -> String.valueOf(i), 4);
        AsyncPipe<String> p = new AsyncProgressPipe<>(p1, 3, v -> progressLog.add(v));
        TerminalPipe p2 = new AsyncConsumerPipe<>(p);
        ) {
      p2.start();
  
      // Must have between 9 to 33 items (progress is checked every 3 items on 4 threads, so "jumps" range from 3 to 12)
      assertTrue(progressLog.size() >= 9 && progressLog.size() <= 34);
      // Must start with 0 and end with 100
      assertEquals(progressLog.get(0), 0);
      assertEquals(progressLog.get(progressLog.size() - 1), 100);
      // Monotonicity
      assertStrictlyMonotonous(progressLog);
    }
  }

  @Test
  public void testProgressSingleItem() throws Exception {
    List<Integer> progressLog = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncPipe<String> p1 = new AsyncSeqGenPipe<>(1, i -> String.valueOf(i), 2);
        AsyncPipe<String> p = new AsyncProgressPipe<>(p1, 1, v -> progressLog.add(v));
        TerminalPipe p2 = new AsyncConsumerPipe<>(p);
        ) {
      p2.start();

      // Must have around 2 items (0 and 100)
      assertEquals(Lists.newArrayList(0, 100), progressLog);
    }
  }

  @Test
  public void testProgressNoItems() throws Exception {
    List<Integer> progressLog = Collections.synchronizedList(new ArrayList<>());
    try (
        AsyncPipe<String> p1 = new AsyncEmptyPipe<>();
        AsyncPipe<String> p = new AsyncProgressPipe<>(p1, 1, v -> progressLog.add(v));
        TerminalPipe p2 = new AsyncConsumerPipe<>(p);
        ) {
      p2.start();

      // Must have around 2 items (0 and 100)
      assertEquals(Lists.newArrayList(0, 100), progressLog);
    }
  }

  private void assertStrictlyMonotonous(List<Integer> progressLog) {
    int prev = -1;
    for (int v : progressLog) {
      assertTrue(v > prev);
      prev = v;
    }
  }
}