package org.pipecraft.infra.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests {@link StaticJobScheduler}
 * 
 * @author Eyal Schneider
 */
public class StaticJobSchedulerTest {
  
  @Test
  public void testNoWorkers() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(1, 2, 3), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(0);
    assertTrue(res.isEmpty());
  }

  @Test
  public void testNoJobs() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Collections.emptyList(), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(4);
    assertEquals(4, res.size());
    for (Collection<Integer> c : res) {
      assertTrue(c.isEmpty());
    }
  }
  
  @Test
  public void testSingleWorker() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(1, 2, 4), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(1);
    assertEquals(1, res.size());
    assertEquals(Arrays.asList(4, 2, 1), res.get(0));
  }

  @Test
  public void testBalanceApproximation1() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(2, 3, 1, 5, 4), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(3); // optimal solution has weight=5 per worker
    assertEquals(3, res.size());
    assertTrue(getMaxWorkerWeight(res) <= 4.0/ 3.0 * 5.0);
    assertDescendingJobOrder(res);
  }

  @Test
  public void testBalanceApproximation2() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(1, 1, 1, 1, 1, 1, 6), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(2); // optimal solution has weight=6 per worker
    assertEquals(2, res.size());
    assertTrue(getMaxWorkerWeight(res) <= 4.0/ 3.0 * 5.0);
    assertDescendingJobOrder(res);
  }

  @Test
  public void testBalanceApproximation3() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(1, 1, 1, 1, 1, 1, 6), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(2); // optimal solution has weight=6 per worker
    assertEquals(2, res.size());
    assertTrue(getMaxWorkerWeight(res) <= 4.0/ 3.0 * 5.0);
    assertDescendingJobOrder(res);
  }

  @Test
  public void testBalanceApproximation4() {
    StaticJobScheduler<Integer> js = new StaticJobScheduler<>(Arrays.asList(1, 2, 3, 4, 5), Number::doubleValue);
    List<Collection<Integer>> res = js.schedule(2); // optimal solution has weight=8 in max worker
    assertEquals(2, res.size());
    assertTrue(getMaxWorkerWeight(res) <= 4.0/ 3.0 * 8.0);
    assertDescendingJobOrder(res);
  }

  private static void assertDescendingJobOrder(List<Collection<Integer>> partition) {
    for (Collection<Integer> c : partition) {
      int prev = Integer.MAX_VALUE;
      for (int n : c) {
        assertTrue(prev >= n);
        prev = n;
      }
    }
  }

  private static int getMaxWorkerWeight(List<Collection<Integer>> partition) {
    int max = 0;
    for (Collection<Integer> c : partition) {
      int sum = c.stream().mapToInt(v -> v).sum();
      max = Math.max(sum, max);
    }
    
    return max;
  }
}
