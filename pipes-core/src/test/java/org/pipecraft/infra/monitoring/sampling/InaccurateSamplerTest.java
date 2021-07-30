package org.pipecraft.infra.monitoring.sampling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class InaccurateSamplerTest {
  @Test
  public void testEmpty() {
    InaccurateSampler<String> s = new InaccurateSampler<String>(10);
    for(String str : s.getSnapshot())
      assertNull(str);
  }

  @Test
  public void testSingleThreadSingleValue() {
    InaccurateSampler<Integer> s = new InaccurateSampler<Integer>(10);

    s.newValue(100);
      
    Set<Integer> storedValues = collectIntoSet(s);    
    assertEquals(new HashSet<Integer>(Arrays.asList(null,100)), storedValues);
  }

  @Test
  public void testSingleThread() {
    InaccurateSampler<Integer> s = new InaccurateSampler<Integer>(10);

    for(int i = 1000000; i > 0; i--)
      s.newValue(i);
      
    Set<Integer> storedValues = collectIntoSet(s);    
    assertEquals(new HashSet<Integer>(Arrays.asList(1,2,3,4,5,6,7,8,9,10)), storedValues);
  }

  @Test
  public void testMultiThreadNoInterference() throws InterruptedException {
    final InaccurateSampler<Integer> s = new InaccurateSampler<Integer>(10);
    Thread[] threads = new Thread[s.size()];
    
    for(int i = 0; i < threads.length; i++) {
      final int id = i;
      threads[i] = new Thread() {
        public void run() {
          try {
            Thread.sleep(250 * id);
            s.newValue(id);
          } catch (InterruptedException e) {
          }
        }
      };
    }
    
    for(Thread t : threads)
      t.start();

    for(Thread t : threads)
      t.join();

    Set<Integer> storedValues = collectIntoSet(s);
    assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3,4,5,6,7,8,9)), storedValues);
  }

  @Test
  public void testMultiThread() throws InterruptedException {
    final InaccurateSampler<Integer> s = new InaccurateSampler<Integer>(10);
    Thread[] threads = new Thread[5];
    
    for(int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        public void run() {
          for (int i = 50000000; i > 0; i--)
            s.newValue(i);
        }
      };
    }
    
    for(Thread t : threads)
      t.start();

    for(Thread t : threads)
      t.join();

    Set<Integer> storedValues = collectIntoSet(s);
    assertTrue(Collections.max(storedValues) <= threads.length * s.size());
  }

  private static <T> Set<T> collectIntoSet(InaccurateSampler<T> s) {
    return new HashSet<T>(s.getSnapshot());
  }
}