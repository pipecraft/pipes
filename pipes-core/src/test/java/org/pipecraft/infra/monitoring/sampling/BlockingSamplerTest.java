package org.pipecraft.infra.monitoring.sampling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class BlockingSamplerTest {
  @Test
  public void testEmpty() {
    Sampler<String> s = new BlockingSampler<String>(10);
    for(String str : s.getSnapshot())
      assertNull(str);
  }

  @Test
  public void testSingleThread() {
    Sampler<Integer> s = new BlockingSampler<Integer>(10);

    s.newValue(300);
    s.newValue(200);
    s.newValue(100);
      
    List<Integer> values = new ArrayList<Integer>(s.getSnapshot());
    assertEquals(100, (int)values.get(0));
    assertEquals(200, (int)values.get(1));
    assertEquals(300, (int)values.get(2));
    for (int i = 3; i < 10; i++)
      assertNull(values.get(i));
  }

  @Test
  public void testMultiThread() throws InterruptedException {
    final Sampler<Integer> s = new BlockingSampler<Integer>(1000000);
    Thread[] threads = new Thread[10];
    final AtomicInteger counter = new AtomicInteger();
    
    for(int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        public void run() {
          for (int i = 100000; i > 0; i--)
            s.newValue(counter.getAndIncrement());
        }
      };
    }
    
    for(Thread t : threads)
      t.start();

    for(Thread t : threads)
      t.join();

    Set<Integer> storedValues = new HashSet<Integer>(s.getSnapshot());
    for(int i = 0; i < 1000000; i++) {
      assertTrue(storedValues.contains(i));
    }
  }
}