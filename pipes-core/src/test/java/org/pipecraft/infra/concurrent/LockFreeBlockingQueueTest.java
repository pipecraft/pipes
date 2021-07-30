package org.pipecraft.infra.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.pipecraft.pipes.AsyncTester;

/**
 * Tests {@link LockFreeBlockingQueue}
 *
 * @author Eyal Schneider
 */
public class LockFreeBlockingQueueTest {
  @Test
  public void testFIFO() throws InterruptedException {
    LockFreeBlockingQueue<Integer> q = new LockFreeBlockingQueue<>(10);
    q.put(8);
    q.put(4);
    q.put(2);
    q.put(2);
    assertEquals(8, q.take());
    q.put(1);
    assertEquals(4, q.take());
    assertEquals(2, q.take());
    assertEquals(2, q.take());
    assertEquals(1, q.take());
    assertNull(q.poll());
  }
  
  @Test
  public void testOfferPollNoResult() throws InterruptedException {
    LockFreeBlockingQueue<Integer> q = new LockFreeBlockingQueue<>(10);
    assertNull(q.poll());
    for (int i = 0; i < 10; i++) {
      q.offer(1);
    }
    assertFalse(q.offer(1));
  }
  
  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  public void testMultiConsumerMultiProducer() throws InterruptedException {
    LockFreeBlockingQueue<Integer> q = new LockFreeBlockingQueue<>(1000);

    final int prodThreadCount = 10;
    final int itemsPerProd = 100_000;
    final int consThreadCount = 5;
    final int itemsPerConsumer = prodThreadCount * itemsPerProd / consThreadCount;
    if ((prodThreadCount * itemsPerProd) % consThreadCount != 0) {
      fail("Incorrect test settings");
    }
    
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(prodThreadCount + consThreadCount);

    // Producers
    AsyncTester[] prod = new AsyncTester[prodThreadCount];
    for (int i = 0; i < prod.length; i++) {
      final int prodInd = i;
      prod[i] = new AsyncTester(() -> {
        try {
          startLatch.countDown();
          startLatch.await();
          for (int j = prodInd * itemsPerProd; j < (prodInd + 1) * itemsPerProd; j++) {
            q.put(j);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException("Unexpected interruption" , e);
        } finally {
          endLatch.countDown();
        }
      });
    }

    for (AsyncTester t : prod) {
      t.start();
    }
    
    // Consumers
    Set<Integer> consumedItems = Collections.synchronizedSet(new HashSet<>());
    AsyncTester[] cons = new AsyncTester[consThreadCount];
    for (int i = 0; i < cons.length; i++) {
      cons[i] = new AsyncTester(() -> {
        try {
          startLatch.countDown();
          startLatch.await();
          for (int j = 0; j < itemsPerConsumer; j++) {
            Integer v = q.take();
            consumedItems.add(v);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException("Unexpected interruption" , e);
        } finally {
          endLatch.countDown();
        }
      });
    }

    for (AsyncTester t : cons) {
      t.start();
    }

    // Start
    startLatch.countDown();
    endLatch.await();
    
    for (AsyncTester t : prod) {
      t.test();
    }
    for (AsyncTester t : cons) {
      t.test();
    }
    
    assertEquals(prodThreadCount * itemsPerProd, consumedItems.size());
    assertEquals(prodThreadCount * itemsPerProd - 1, consumedItems.stream().max(Integer::compareTo).get());
    assertEquals(0, consumedItems.stream().min(Integer::compareTo).get());
  }
}
