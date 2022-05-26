package org.pipecraft.infra.monitoring.quantile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tdunning.math.stats.MergingDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QuantileDigestTest {
  private static final Random r = new Random(100);

  @BeforeEach
  public void resetSeed() {
    r.setSeed(100);
  }

  @Test
  public void testAddDigests() {
    QuantileDigest digest1 = new QuantileDigest(100);
    QuantileDigest digest2 = new QuantileDigest(100);

    for (int i = 0; i < 10000; i++) {
      digest1.add(r.nextGaussian() * 100 + 500);
    }

    for (int i = 0; i < 10000; i++) {
      digest2.add(r.nextGaussian() * 100 + 700);
    }

    digest1.add(digest2);

    assertEquals(digest1.size(), 20000);
  }

  @Test
  public void testAddMultipleDigests() {
    int insertCount = 10000;
    QuantileDigest digest1 = new QuantileDigest(100);

    List<QuantileDigest> others = Arrays.asList(
            new QuantileDigest(100),
            new QuantileDigest(100),
            new QuantileDigest(100),
            new QuantileDigest(100)
    );

    for (int i = 0; i < insertCount; i++) {
      digest1.add(r.nextGaussian() * 100 + 500);
    }

    for (QuantileDigest digest: others) {
      for (int i = 0; i < insertCount; i++) {
        digest.add(r.nextGaussian() * 100 + 700);
      }
    }

    digest1.add(others);

    assertEquals(digest1.size(), (long) insertCount * (others.size() + 1));
  }

  @Test
  public void testWrap() {
    int insertCount = 10000;

    MergingDigest digest = new MergingDigest(100);
    for (int i = 0; i < insertCount; i++) {
      digest.add(r.nextGaussian() * 100 + 700);
    }

    QuantileDigest wrapped = QuantileDigest.wrap(digest);
    for (int i = 0; i < insertCount; i++) {
      digest.add(r.nextGaussian() * 100 + 700);
    }

    assertEquals(digest.size(), 2 * insertCount);
    assertEquals(wrapped.size(), insertCount);
  }

  @Test
  public void testWrapEmpty() {
    int insertCount = 10000;
    MergingDigest digest = new MergingDigest(100);
    QuantileDigest wrapped = QuantileDigest.wrap(digest);
    for (int i = 0; i < insertCount; i++) {
      digest.add(r.nextGaussian() * 100 + 700);
    }

    assertEquals(digest.size(), insertCount);
    assertEquals(wrapped.size(), 0);
  }

  @Test
  public void testDeserialization() {
    QuantileDigest digest = new QuantileDigest(100);

    for (int i = 0; i < 10000; i++) {
      digest.add(r.nextInt(100));
    }

    QuantileDigest deserialized = QuantileDigest.fromBytes(digest.asReadOnlyBuffer());

    assertEquals(deserialized.size(), digest.size());
    assertEquals(deserialized.quantile(0.5), digest.quantile(0.5));
    assertEquals(deserialized.quantile(0.99), digest.quantile(0.99));
  }

  @Test
  public void testGetAndReset() {
    QuantileDigest digest = new QuantileDigest(100);

    for (int i = 0; i < 10000; i++) {
      digest.add(r.nextInt(100));
    }

    QuantileDigest digestCopy = QuantileDigest.fromBytes(digest.asReadOnlyBuffer());
    QuantileDigest resetCopy = digest.reset();

    assertEquals(0, digest.size());
    assertEquals(digestCopy.quantile(0.5), resetCopy.quantile(0.5));
    assertEquals(digestCopy.quantile(0.99), resetCopy.quantile(0.99));
  }
}
