package org.pipecraft.infra.monitoring.quantile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SlidingWindowQuantileEstimatorTest {
  private static final Random r = new Random(100);
  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  @BeforeEach
  public void resetSeed() {
    r.setSeed(100);

    executorService.shutdown();
    executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Test
  public void testEmptyDigest() {
    int capacity = 2;
    SlidingWindowQuantileDigest digest = new SlidingWindowQuantileDigest(10, 100, 1.5, -1, capacity, executorService);

    digest.publishToJournal();
    digest.publishToJournal();
    digest.publishToJournal();
    digest.publishToJournal();
    digest.publishToJournal();

    ConcurrentQuantileEstimator merged = digest.summarize();

    assertEquals(merged.size(), 0);
  }

  @Test
  public void testSummarize() {
    SlidingWindowQuantileDigest digest = new SlidingWindowQuantileDigest(10, 100, -1, 5, executorService);

    for (int i = 0; i < 10000; i++) {
      digest.add(r.nextInt(100));
    }

    QuantileDigest summary = digest.summarize();

    assertEquals(digest.getCompression(), 100);
    assertEquals(digest.quantile(0.5), summary.quantile(0.5));

    for (int i = 0; i < 10000; i++) {
      summary.add(r.nextInt(100));
    }

    assertEquals(digest.size(), 10000);
    assertEquals(summary.size(), 20000);
  }

  @Test
  public void testJournalEviction() throws IOException {
    double[] valuesConcat;

    int capacity = 2;
    SlidingWindowQuantileDigest digest = new SlidingWindowQuantileDigest(10, 100, 1.5, -1, capacity, executorService);

    // add data, check consistency -- journal length is 0
    double[] values1 = addData(digest, 500, 1000);
    assertPercentileWithinEpsilon(digest.summarize(0), values1, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(1), values1, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(2), values1, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(),  values1, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest,              values1, 0.5, 0.01);

    // archive, add more data, check consistency -- journal length is 1
    digest.publishToJournal();
    double[] values2 = addData(digest, 1000, 1000);
    valuesConcat = ArrayUtils.addAll(values1, values2);
    assertPercentileWithinEpsilon(digest.summarize(0), values2,      0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(1), valuesConcat, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(2), valuesConcat, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(),  valuesConcat, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest,              valuesConcat, 0.5, 0.01);

    // archive, add more data, check consistency -- journal length is 2 (max)
    digest.publishToJournal();
    double[] values3 = addData(digest, 1500, 1000);
    valuesConcat = ArrayUtils.addAll(valuesConcat, values3);
    double[] valuesConcat23 = ArrayUtils.addAll(values3, values2);
    assertPercentileWithinEpsilon(digest.summarize(0), values3, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(1), valuesConcat23, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(2), valuesConcat, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest.summarize(),  valuesConcat, 0.5, 0.01);
    assertPercentileWithinEpsilon(digest,              valuesConcat, 0.5, 0.01);
    assertEquals(digest.size(), 3000);

    // archive, check consistency -- first set of data got evicted
    digest.publishToJournal();
    assertPercentileWithinEpsilon(digest, valuesConcat23, 0.5, 0.01);
    assertEquals(digest.size(), 2000);

    // archive, check consistency -- second set of data got evicted
    digest.publishToJournal();
    assertPercentileWithinEpsilon(digest, values3, 0.5, 0.01);
    assertEquals(digest.size(), 1000);

    // archive, check consistency -- all data is evicted -- back to pristine state
    digest.publishToJournal();
    assertEquals(digest.size(), 0);
  }

  private double[] addData(SlidingWindowQuantileDigest digest, double mean, int count) {
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      double val = r.nextGaussian() * 20 + mean;
      digest.add(val);
      values[i] = val;
    }

    return values;
  }

  private void assertPercentileWithinEpsilon(
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
