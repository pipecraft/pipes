package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.inter.SyncToAsyncPipe;
import org.pipecraft.infra.concurrent.FailableConsumer;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.inter.CallbackPipe;
import org.pipecraft.pipes.sync.inter.MapPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.ConsumerPipe;
import org.pipecraft.pipes.terminal.ParallelConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * Tests {@link ParallelConsumerPipe}
 * 
 * @author Eyal Schneider
 */
public class ParallelConsumerPipeTest {
  @Test
  public void testSingle() throws Exception {
    Collection<Integer> bag = HashMultiset.create();
    FailableConsumer<Integer, PipeException> callback = v -> {
      if (v != null) {
        synchronized(bag) {
          bag.add(v);
        }
      }
    };
    
    try(
        Pipe<Integer> source = new CollectionReaderPipe<>(10, 20, 30);
        Pipe<Integer> cp = new CallbackPipe<>(source, callback);
        ParallelConsumerPipe p = new ParallelConsumerPipe(10, cp);
    ) {
      p.start();
    }
    
    Multiset<Integer> expected = HashMultiset.create();
    expected.add(10);
    expected.add(20);
    expected.add(30);
    assertEquals(expected, bag);
  }
  
  @Test
  public void testMultiple() throws Exception {
    Collection<Number> bag = HashMultiset.create();
    FailableConsumer<Number, PipeException> callback = v -> {
      if (v != null) {
        synchronized(bag) {
          bag.add(v);
        }
      }
    };

    try(
        Pipe<Integer> p1 = new CallbackPipe<>(new CollectionReaderPipe<>(10, 20, 30), callback);
        Pipe<Double> p2 = new CallbackPipe<>(new CollectionReaderPipe<>(15.0, 20.0, 35.0, 45.0), callback);
        AsyncPipe<Double> p2A = new SyncToAsyncPipe<>(Collections.singleton(() -> p2), 1);
        Pipe<Integer> p3 = new CallbackPipe<>(new CollectionReaderPipe<>(100, 20, 200), callback);
        TerminalPipe p4 = new ConsumerPipe<>(new CallbackPipe<>(new CollectionReaderPipe<>(1, 2, 4, 8), callback));
        ParallelConsumerPipe p = new ParallelConsumerPipe(2, p1, p2, p3, p4);
    ) {
      p.start();
    }

    Multiset<Number> expected = HashMultiset.create();
    expected.add(1);
    expected.add(2);
    expected.add(4);
    expected.add(8);
    expected.add(10);
    expected.add(15.0);
    expected.add(20, 2);
    expected.add(20.0, 1);
    expected.add(30);
    expected.add(35.0);
    expected.add(45.0);
    expected.add(100);
    expected.add(200);
    assertEquals(expected, bag);
  }

  @Test
  public void testErrors() throws Exception {
    try(
        Pipe<Integer> p1 = new CollectionReaderPipe<>(10, 20, 30);
        Pipe<Integer> p2 = new CollectionReaderPipe<>(15, 20, 35, 45);
        Pipe<Integer> p2WithErr = new MapPipe<>(p2, v -> {
          if (v == 35) {
            throw new ValidationPipeException("Test");
          }
          return v;
        });
        Pipe<Integer> p3 = new CollectionReaderPipe<>(100, 20, 200);
        ParallelConsumerPipe p = new ParallelConsumerPipe(2, p1, p2WithErr, p3);
    ) {
      assertThrows(ValidationPipeException.class, () -> p.start());
    }    
  }

  @Test
  public void testTerminationAction() throws Exception {
    AtomicInteger termination = new AtomicInteger();
    Collection<Number> bag = HashMultiset.create();
    FailableConsumer<Number, PipeException> callback = v -> {
      if (v != null) {
        synchronized(bag) {
          bag.add(v);
        }
      }
    };
    Runnable terminationAction = () -> {
      termination.incrementAndGet();
      synchronized(bag) {
        if (bag.size() != 10) {
          termination.incrementAndGet(); // To make the test fail
        }
      }
    };
    
    try(
        Pipe<Integer> p1 = new CallbackPipe<>(new CollectionReaderPipe<>(10, 20, 30), callback);
        Pipe<Double> p2 = new CallbackPipe<>(new CollectionReaderPipe<>(15.0, 20.0, 35.0, 45.0), callback);
        Pipe<Integer> p3 = new CallbackPipe<>(new CollectionReaderPipe<>(100, 20, 200), callback);
        ParallelConsumerPipe p = new ParallelConsumerPipe(2, Arrays.asList(p1, p2, p3), terminationAction);
    ) {
      p.start();
    }

    Multiset<Number> expected = HashMultiset.create();
    expected.add(10);
    expected.add(15.0);
    expected.add(20, 2);
    expected.add(20.0, 1);
    expected.add(30);
    expected.add(35.0);
    expected.add(45.0);
    expected.add(100);
    expected.add(200);
    assertEquals(expected, bag);
    assertEquals(1, termination.get());
  }

}
