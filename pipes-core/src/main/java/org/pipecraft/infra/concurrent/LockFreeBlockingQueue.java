package org.pipecraft.infra.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A blocking queue implementation which wraps the efficient ConcurrentLinkedQueue,
 * and adds adaptive yields/sleeps in order to achieve a blocking behavior without
 * using locks.
 * 
 * The queue turns out to be more efficient than standard blocking queues ({@link ArrayBlockingQueue}
 * and {@link LinkedBlockingQueue}), specially in cases of small queue capacity, or when producers and
 * consumers aren't balanced in their rates.
 * 
 * This implementation is optimized for overall throughput, but it's not intended for
 * cases where latency of individual put(E) or take() operations is important.
 * Therefore, this implementation is specially suitable for multithreaded pipelines.
 * 
 * @param <E> The queue item data type
 * 
 * @author Eyal Schneider
 */
public class LockFreeBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
  private static final int YIELD_MAX = 64;
  private static final int SLEEP_INITIAL_MS = 1;
  private static final int SLEEP_MAX_MS = 1024;
  private static final double SLEEP_FACTOR = 2.0;
  
  private final ConcurrentLinkedQueue<E> queue;
  private final AtomicInteger size = new AtomicInteger();
  private final int approxSizeLimit;
 
  /**
   * Constructor
   * 
   * @param approxSizeLimit The required capacity of the queue. Note that due to the 
   * lock-free behavior of this class, this is only an approximate limit, and there may
   * be deviations. 
   */
  public LockFreeBlockingQueue(int approxSizeLimit) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.approxSizeLimit = approxSizeLimit;
  }
  
  @Override
  public boolean offer(E e) {
    if (size.get() >= approxSizeLimit) {
      return false;
    }
    queue.add(e);
    size.incrementAndGet();
    return true;
  }

  // Note that time units resolution of this method is milliseconds, so there's no use in using finer time units.
  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    long startMs = System.currentTimeMillis();
    long maxDurationMs = unit.toMillis(timeout);
    if (offer(e)) {
      return true;
    }
    for (int i = 0; i < YIELD_MAX; i++) {
      Thread.yield();
      if (offer(e)) {
        return true;
      }
      checkInterruption();
    }

    long sleepMs = SLEEP_INITIAL_MS;
    while (true) {
      sleep(sleepMs, startMs, maxDurationMs);
      if (offer(e)) {
        return true;
      }
      if (System.currentTimeMillis() - startMs > maxDurationMs) {
        return false;
      }
      checkInterruption();
      sleepMs = Math.min(SLEEP_MAX_MS, (long) (sleepMs * SLEEP_FACTOR));
    }
  }

  @Override
  public E poll() {
    E res = queue.poll();
    if (res != null) {
      size.decrementAndGet();
    }
    return res;
  }

  // Note that time units resolution of this method is milliseconds, so there's no use in using finer time units.
  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    E value;
    long startMs = System.currentTimeMillis();
    long maxDurationMs = unit.toMillis(timeout);

    if ((value = poll()) != null) {
      return value;
    }
    
    for (int i = 0; i < YIELD_MAX; i++) {
      Thread.yield();
      if ((value = poll()) != null) {
        return value;
      }
      checkInterruption();
    }

    long sleepMs = SLEEP_INITIAL_MS;
    while (true) {
      sleep(sleepMs, startMs, maxDurationMs);
      if ((value = poll()) != null) {
        return value;
      }
      if (System.currentTimeMillis() - startMs > maxDurationMs) {
        return null;
      }
      checkInterruption();
      sleepMs = Math.min(SLEEP_MAX_MS, (long)(sleepMs * SLEEP_FACTOR));
    }
  }

  @Override
  public E peek() {
    return queue.peek();
  }

  @Override
  public Iterator<E> iterator() {
    return queue.iterator();
  }

  @Override
  public int size() {
    return size.get();
  }

  @Override
  public void put(E e) throws InterruptedException {
    if (offer(e)) {
      return;
    }
    for (int i = 0; i < YIELD_MAX; i++) {
      Thread.yield();
      if (offer(e)) {
        return;
      }
      checkInterruption();
    }

    long sleepMs = SLEEP_INITIAL_MS;
    while (true) {
      Thread.sleep(sleepMs);
      if (offer(e)) {
        return;
      }
      checkInterruption();
      sleepMs = Math.min(SLEEP_MAX_MS, (long) (sleepMs * SLEEP_FACTOR));
    }
  }

  @Override
  public E take() throws InterruptedException {
    E value;
    if ((value = poll()) != null) {
      return value;
    }
    
    for (int i = 0; i < YIELD_MAX; i++) {
      Thread.yield();
      if ((value = poll()) != null) {
        return value;
      }
      checkInterruption();
    }

    long sleepMs = SLEEP_INITIAL_MS;
    while (true) {
      Thread.sleep(sleepMs);
      if ((value = poll()) != null) {
        return value;
      }
      checkInterruption();
      sleepMs = Math.min(SLEEP_MAX_MS, (long)(sleepMs * SLEEP_FACTOR));
    }
  }

  @Override
  public int remainingCapacity() {
    return approxSizeLimit - size.get();
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    int count = 0;
    E e;
    while (count < maxElements && (e = poll()) != null) {
      c.add(e);
      count++;
    }
    return count;
  }  
  
  private static void checkInterruption() throws InterruptedException {
    if (Thread.interrupted()) { // We clear the interruption flag here
      throw new InterruptedException();
    }
  }
  
  private static void sleep(long sleepMs, long startMs, long maxDurationMs) throws InterruptedException {
    Thread.sleep(Math.min(sleepMs,  Math.max(0, startMs + maxDurationMs - System.currentTimeMillis())));    
  }
}
