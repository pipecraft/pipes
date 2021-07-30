package org.pipecraft.infra.monitoring.quantile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MultiQuantileDigestTest {
  private static final Random r = new Random(100);

  @BeforeEach
  public void resetSeed() {
    r.setSeed(100);
  }

  @Test
  public void testInvalidParams() {
    assertThrows(IllegalArgumentException.class, () -> new MultiQuantileDigest(0));
    assertThrows(IllegalArgumentException.class, () -> new MultiQuantileDigest(1,  0));
    assertThrows(IllegalArgumentException.class, () -> new MultiQuantileDigest(1,  1, 0));
  }

  @Test
  void testReset() {
    MultiQuantileDigest digest = new MultiQuantileDigest(100);

    for (int i = 0; i < 10000; i++) {
      digest.add(r.nextInt(100));
    }

    QuantileDigest preReset = digest.reset();

    assertEquals(digest.size(), 0);
    assertEquals(preReset.size(), 10000);
  }

  @Test
  public void testSummarize() {
    MultiQuantileDigest digest = new MultiQuantileDigest(100);

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
}
