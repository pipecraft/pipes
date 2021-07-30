package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.sync.inter.ProgressPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * A test for the ProgressPipe, which tracks processing progress
 * 
 * @author Eyal Schneider
 */
public class ProgressPipeTest {
  @Test
  public void testProgress() throws Exception {
    List<Integer> progressLog = new ArrayList<>();
    CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>("2", "3", "4", "5");
    try (ProgressPipe<String> p = new ProgressPipe<>(p1, 1, v -> progressLog.add(v))) {
      p.start();
      while(p.next() != null);
      assertEquals(Lists.newArrayList(0, 25, 50, 75, 100), progressLog);
    }
  }

  @Test
  public void testProgressSingleItem() throws Exception {
    List<Integer> progressLog = new ArrayList<>();
    CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>("2");
    try (ProgressPipe<String> p = new ProgressPipe<>(p1, 1, v -> progressLog.add(v))) {
      p.start();
      while(p.next() != null);
      assertEquals(Lists.newArrayList(0, 100), progressLog);
    }
  }

  @Test
  public void testProgressNoItems() throws Exception {
    List<Integer> progressLog = new ArrayList<>();
    CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>();
    try (ProgressPipe<String> p = new ProgressPipe<>(p1, 1, v -> progressLog.add(v))) {
      p.start();
      while(p.next() != null);
      assertEquals(Lists.newArrayList(0, 100), progressLog);
    }
  }

}