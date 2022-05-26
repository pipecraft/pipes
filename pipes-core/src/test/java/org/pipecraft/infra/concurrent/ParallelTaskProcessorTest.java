package org.pipecraft.infra.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

/**
 * Unit test for ParallelTaskProcessor
 * 
 * @author Eyal Schneider
 *
 */
public class ParallelTaskProcessorTest {

  @Test
  public void testNonFailableOneThread() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ParallelTaskProcessor.run(Arrays.asList(1,2,3,4,5), 1, completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
  }

  @Test
  public void testNonFailableMultipleThreads() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ParallelTaskProcessor.run(Arrays.asList(1,2,3,4,5), 4, completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
  }

  @Test
  public void testNonFailableSuppliedExecutor() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ExecutorService ex = Executors.newFixedThreadPool(4);
    ParallelTaskProcessor.run(ex, Arrays.asList(1,2,3,4,5), completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
    assertFalse(ex.isShutdown());
  }
  
  @Test
  public void testNonFailableNoItems() throws Exception {
    AtomicBoolean processorInvoked = new AtomicBoolean(false);
    ParallelTaskProcessor.run(Collections.emptyList(), 10, v -> {processorInvoked.set(true);});
    assertFalse(processorInvoked.get());
  }

  @Test
  public void testNonFailableRuntimeError() throws Exception {
    assertThrows(TestRuntimeException.class,
        () -> ParallelTaskProcessor.run(Arrays.asList(1,2,3,4,5), 4, v -> { throw new TestRuntimeException();}));
  }
  
  @Test
  public void testFailableOneThread() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 1, completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
  }

  @Test
  public void testFailableMultipleThreads() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 4, completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
  }

  @Test
  public void testFailableSuppliedExecutor() throws Exception {
    Set<Integer> completed = Collections.synchronizedSet(new HashSet<>());
    ExecutorService ex = Executors.newFixedThreadPool(4);
    ParallelTaskProcessor.runFailable(ex, Arrays.asList(1,2,3,4,5), completed::add);
    assertEquals(Sets.newHashSet(1,2,3,4,5), completed);
    assertFalse(ex.isShutdown());
  }
  
  @Test
  public void testFailableNoItems() throws Exception {
    AtomicBoolean processorInvoked = new AtomicBoolean(false);
    ParallelTaskProcessor.runFailable(Collections.emptyList(), 10, v -> {processorInvoked.set(true);});
    assertFalse(processorInvoked.get());
  }
  
  @Test
  public void testFailableWithCheckedException() throws Exception {
    assertThrows(IOException.class, 
        () -> ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 5, v -> {if (v == 5) throw new IOException();}));
  }

  @Test
  public void testFailableWithRuntimeException() throws Exception {
    assertThrows(TestRuntimeException.class, 
        () -> ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 5, v -> {if (v == 5) throw new TestRuntimeException();}));
  }

  @Test
  public void testFailableExitImmediatelyOnError() throws Exception {
    AtomicBoolean oneSecEllapsed = new AtomicBoolean(false);
    try {
      ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 5, v -> {
        if (v == 5) {
          throw new IOException();
        } else {
          Thread.sleep(1000);
          oneSecEllapsed.set(true);
        };
      });
      fail("Should have thrown IOException");
    } catch (IOException e) {
      assertFalse(oneSecEllapsed.get());
    }
  }

  @Test
  public void testInterruptedException() throws Exception {
    AtomicBoolean oneSecEllapsed = new AtomicBoolean(false);
    ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
    try {
      Thread mainThread = Thread.currentThread();
      ex.schedule(mainThread::interrupt, 100, TimeUnit.MILLISECONDS);
      
      ParallelTaskProcessor.runFailable(Arrays.asList(1,2,3,4,5), 5, v -> {
        try {
          Thread.sleep(1000);
          oneSecEllapsed.set(true);
        } catch (InterruptedException e) { } // Exits when interrupted
      });

      fail("Should have thrown InterruptedException");
    } catch (InterruptedException e) {
      assertFalse(oneSecEllapsed.get()); // We want to make sure that the tasks were interrupted, and not waited for
    } finally {
      ex.shutdownNow();
      ex.awaitTermination(2, TimeUnit.SECONDS);
    }
  }

  private static class TestRuntimeException extends RuntimeException {
  }
}
