package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.inter.SyncToAsyncPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.sync.source.ErrorPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

public class AsyncEnqueuingSharderPipeTest {
  private static final Integer ERR_MARKER = new Integer(0);
  private static final Integer SUCCESS_MARKER = new Integer(0);
  
  @Test
  public void testWriteReadNoBlocking() throws Exception {
    List<BlockingQueue<Integer>> queues = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      queues.add(new ArrayBlockingQueue<>(1000));
    }
    AtomicReference<Integer> endMarkerFound = new AtomicReference<Integer>();
    try (
        AsyncPipe<Integer> gen = new AsyncSeqGenPipe<>(1000, i -> i.intValue(), 5);
        AsyncEnqueuingSharderPipe<Integer> p = new AsyncEnqueuingSharderPipe<>(gen, queues, SUCCESS_MARKER, ERR_MARKER);
        ) {
      p.start(); // we can afford a blocking start() here because non of the queues should become full
      Set<Integer> consumedItems = drainQueues(queues, endMarkerFound);
      assertEquals(1000, consumedItems.size());
      assertEquals(0, consumedItems.stream().min(Integer::compareTo).get());
      assertEquals(999, consumedItems.stream().max(Integer::compareTo).get());
      assertTrue(SUCCESS_MARKER == endMarkerFound.get());
    }
  }

  @Test
  public void testBlocking() throws Exception {
    List<BlockingQueue<Integer>> queues = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      queues.add(new ArrayBlockingQueue<>(1000));
    }
    AtomicReference<Integer> endMarkerFound = new AtomicReference<Integer>();
    
    try (
        AsyncPipe<Integer> gen = new AsyncSeqGenPipe<>(100_000, i -> i.intValue(), 5);
        AsyncEnqueuingSharderPipe<Integer> p = new AsyncEnqueuingSharderPipe<>(gen, queues, SUCCESS_MARKER, ERR_MARKER);
        ) {
      Future<Void> f = p.asyncStart(); 
      Set<Integer> consumedItems = drainQueues(queues, endMarkerFound);
      f.get(); // Assert no exception is thrown
      assertEquals(100_000, consumedItems.size());
      assertEquals(0, consumedItems.stream().min(Integer::compareTo).get());
      assertEquals(99_999, consumedItems.stream().max(Integer::compareTo).get());
      assertTrue(SUCCESS_MARKER == endMarkerFound.get());
    }
  }

  @Test
  public void testError() throws Exception {
    List<BlockingQueue<Integer>> queues = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      queues.add(new ArrayBlockingQueue<>(1000));
    }
    AtomicReference<Integer> endMarkerFound = new AtomicReference<Integer>();
    
    PipeException exception = new ValidationPipeException("Test exception");
    try (
        Pipe<Integer> genP = new SeqGenPipe<>(i -> i.intValue(), 100_000);
        Pipe<Integer> errP = new ErrorPipe<>(exception);
        Pipe<Integer> concatP = ConcatPipe.fromPipes(genP, errP);
        AsyncPipe<Integer> p = SyncToAsyncPipe.fromPipe(concatP);
        AsyncEnqueuingSharderPipe<Integer> sharder = new AsyncEnqueuingSharderPipe<>(p, queues, SUCCESS_MARKER, ERR_MARKER);
        ) {
      Future<Void> f = sharder.asyncStart(); 
      drainQueues(queues, endMarkerFound);
      assertTrue(ERR_MARKER == endMarkerFound.get());
      try {
        f.get();
        fail("Expected an exception");
      } catch (ExecutionException e) {
        assertEquals(exception, e.getCause());
      }
    }
  }

  @Test
  public void testShardingFunc() throws Exception {
    List<BlockingQueue<Integer>> queues = new ArrayList<>();
    for (int i = 0; i < 4; i++) { // 4 queues, where queue #i is intended for inputs v having v % 4 = i
      queues.add(new ArrayBlockingQueue<>(1000)); // queues big enough to prevent blocking writes
    }
    AtomicReference<Integer> endMarkerFound = new AtomicReference<Integer>();
    try (
        AsyncPipe<Integer> gen = new AsyncSeqGenPipe<>(1000, i -> i.intValue(), 5);
        AsyncEnqueuingSharderPipe<Integer> p = new AsyncEnqueuingSharderPipe<>(gen, queues, v -> v % queues.size(), SUCCESS_MARKER, ERR_MARKER);
        ) {
      p.start(); // we can afford a blocking start() here because non of the queues should become full
      for (int rem = 0; rem < 4; rem++) {
        final int remainder = rem;
        Set<Integer> consumed = drainQueues(Collections.singleton(queues.get(rem)), endMarkerFound);
        assertTrue(SUCCESS_MARKER == endMarkerFound.get());
        consumed.forEach(v -> assertTrue(v % 4 == remainder));
      }
    }
  }

  // Uses one thread per queue to consume all queue items (till completion markers or a read timeout)
  // Expects to see no duplicates.
  private static Set<Integer> drainQueues(Collection<? extends BlockingQueue<Integer>> queues, AtomicReference<Integer> markerFound) throws RuntimeException, InterruptedException {
    Set<Integer> res = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger totalNonUnique = new AtomicInteger();
    Map<Integer, Void> endMarkers = Collections.synchronizedMap(new IdentityHashMap<>());
    
    ParallelTaskProcessor.runFailable(queues, queues.size(), q -> {
      try {
        while (true) {
          Integer next = q.poll(30, TimeUnit.SECONDS);
          if (next == null) {
            fail("Timeout while reading from queue");
          }
          
          if (next == SUCCESS_MARKER || next == ERR_MARKER) {
            endMarkers.put(next, null);
            if (endMarkers.size() == 2) {
              fail("Encountered different end markers in different queues");
            }
            markerFound.set(next);
            break;
          }
          
          totalNonUnique.incrementAndGet();
          res.add(next);
        }
      } catch (InterruptedException e) {
        // Make it interruptible by the parallel task processor by exiting fast
      }
    });
    
    assertEquals(res.size(), totalNonUnique.get());
    return res;
  }
}
