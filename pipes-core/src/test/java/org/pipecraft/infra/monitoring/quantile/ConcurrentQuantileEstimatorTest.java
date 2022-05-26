package org.pipecraft.infra.monitoring.quantile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests shared between all implementations of {@link ConcurrentQuantileEstimator}
 */
public class ConcurrentQuantileEstimatorTest {
  private static final Random r = new Random(100);
  private static List<ConcurrentQuantileEstimator> digests = new ArrayList<>();
  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  @BeforeEach
  public void resetSeed() {
    r.setSeed(100);

    executorService.shutdown();
    executorService = Executors.newSingleThreadScheduledExecutor();

    digests = Arrays.asList(
            new QuantileDigest(100),
            new MultiQuantileDigest(10, 100, 1.5),
            new SlidingWindowQuantileDigest(10, 100, 1.5, -1, 10, executorService)
    );
  }

  @Test
  public void testAdd() {
    for (ConcurrentQuantileEstimator digest: digests) {
      for (int i = 0; i < 100; i++) {
        digest.add(r.nextInt(100));
      }
      assertEquals(digest.size(), 100);
      for (int i = 0; i < 100; i++) {
        digest.add(r.nextInt(100), 2);
      }
      assertEquals(digest.size(), 300);
    }
  }

  @Test
  public void testTryAdd() {
    for (ConcurrentQuantileEstimator digest: digests) {
      for (int i = 0; i < 100; i++) {
        boolean inserted = digest.tryAdd(r.nextInt(100));
        assertTrue(inserted);
      }

      assertEquals(digest.size(), 100);

      for (int i = 0; i < 100; i++) {
        boolean inserted = digest.tryAdd(r.nextInt(100), 2);
        assertTrue(inserted);
      }

      assertEquals(digest.size(), 300);
    }
  }

  @Test
  public void testListQueries() {
    int maxv = 1000;
    for (ConcurrentQuantileEstimator digest: digests) {
      for (int i = 0; i < 100; i++) {
        digest.add(r.nextInt(maxv));
        digest.add(r.nextInt(maxv), 2);
      }

      List<Double> qs = new ArrayList<>();
      for (double q = 0.01; q <= 1; q += 0.01) {
        qs.add(q);
      }

      List<Double> results = digest.quantile(qs);
      for (int i = 0; i < results.size(); i++) {
        assertTrue(Math.abs(results.get(i) - digest.quantile(qs.get(i))) < 0.0000001);
      }

      List<Double> xs = new ArrayList<>();
      for (int x = 1; x <= maxv; x++) {
        xs.add((double) x);
      }

      results = digest.cdf(xs);
      for (int i = 0; i < results.size(); i++) {
        assertTrue(Math.abs(results.get(i) - digest.cdf(xs.get(i))) < 0.0000001);
      }
    }
  }


  @Test
  public void testMultiThread() throws InterruptedException {
    for (ConcurrentQuantileEstimator digest: digests) {
      int threads = 10;
      CountDownLatch latch = new CountDownLatch(1);

      List<Thread> threadList = new ArrayList<>();
      for (int t = 0; t < threads; ++t) {
        Thread thread = new Thread(() -> {
          try {
            latch.await();
          } catch (InterruptedException e) { /**/ }

          for (int i = 0; i < 10000; i++) {
            digest.add(ThreadLocalRandom.current().nextInt(100));
          }
        });

        thread.start();
        threadList.add(thread);
      }

      latch.countDown();
      for (Thread t : threadList) {
        t.join();
      }

      assertEquals(digest.size(), threads * 10000);
    }
  }

  @Test
  public void testMultiThreadTryAdd() throws InterruptedException {
    for (ConcurrentQuantileEstimator digest: digests) {
      int threads = 10;
      CountDownLatch latch = new CountDownLatch(1);

      List<Thread> threadList = new ArrayList<>();
      AtomicInteger insertCount = new AtomicInteger();

      for (int t = 0; t < threads; ++t) {
        Thread thread = new Thread(() -> {
          int localInsertCount = 0;
          try {
            latch.await();
          } catch (InterruptedException e) { /**/ }

          for (int i = 0; i < 10000; i++) {
            boolean inserted = digest.tryAdd(ThreadLocalRandom.current().nextInt(100));
            if (inserted) {
              localInsertCount++;
            }
          }

          insertCount.addAndGet(localInsertCount);
        });

        thread.start();
        threadList.add(thread);
      }

      latch.countDown();
      for (Thread t : threadList) {
        t.join();
      }

      assertEquals(digest.size(), insertCount.get());
    }
  }

  @Test
  public void testSingleThreadAccuracy() {
    for (ConcurrentQuantileEstimator digest : digests) {
      double[] values = new double[20000];

      for (int i = 0; i < 10000; i++) {
        double val = r.nextGaussian() * 100 + 500;
        digest.add(val);
        values[i] = val;
      }

      for (int i = 0; i < 10000; i++) {
        double val = r.nextGaussian() * 100 + 700;
        digest.add(val);
        values[i + 10000] = val;
      }

      for (int percentile = 2; percentile < 100; percentile++) {
        assertQuantileWithinEpsilon(digest, values, percentile / 100.0, 0.01);
      }
    }
  }

  @Test
  public void testMultiThreadAccuracy() throws InterruptedException {
    for (ConcurrentQuantileEstimator digest: digests) {
      int threads = 10;
      CountDownLatch latch = new CountDownLatch(1);

      List<Thread> threadList = new ArrayList<>();
      final List<List<Double>> datas = new ArrayList<>();
      for (int t = 0; t < threads; ++t) {
        final List<Double> data = new ArrayList<>();
        datas.add(data);
        Thread thread = new Thread(() -> {
          Random r = new Random(1000);

          try {
            latch.await();
          } catch (InterruptedException e) { /**/ }

          for (int i = 0; i < 1000; i++) {
            double mean = r.nextGaussian() * 30 + 500;
            double val = r.nextGaussian() * 70 + mean;

            digest.add(val);
            data.add(val);
          }
        });

        thread.start();
        threadList.add(thread);
      }

      latch.countDown();
      for (Thread t : threadList) {
        t.join();
      }

      double[] values = new double[1000 * threads];
      int i = 0;
      for (List<Double> l : datas)
        for (Double d : l) {
          values[i++] = d;
        }

      for (int percentile = 2; percentile < 100; percentile++) {
        assertQuantileWithinEpsilon(digest, values, percentile / 100.0, 0.01);
      }
    }
  }

  private void assertQuantileWithinEpsilon(
          ConcurrentQuantileEstimator digest, double[] data, double quantile, double eps) {
    double percentile = quantile * 100.0;
    Percentile perc = new Percentile();
    double low = perc.evaluate(data, percentile - eps * 100);
    double high = perc.evaluate(data, percentile + eps * 100);

    double observed = digest.quantile(quantile);
    assertTrue(digest.quantile(quantile) > low && digest.quantile(quantile) < high,
            "closeness not met: " + low + " < " + observed + " < " + high);
  }
}
