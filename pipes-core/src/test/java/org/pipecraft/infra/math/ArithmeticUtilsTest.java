package org.pipecraft.infra.math;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class ArithmeticUtilsTest {
  @Test
  public void testUnionProbability() {
    List<Pair<List<Double>, Double>> data =
        Arrays.asList(
            new ImmutablePair<>(Arrays.asList(0.5, 0.5), 0.75),
            new ImmutablePair<>(Arrays.asList(0.4, 0.4), 0.64),
            new ImmutablePair<>(Arrays.asList(0.5, 0.5, 0.5), 0.875),
            new ImmutablePair<>(Arrays.asList(0.8, 0.6, 0.7), 0.976),
            new ImmutablePair<>(Arrays.asList(0.18, -0.9, 0.15), -0.3243),
            new ImmutablePair<>(Arrays.asList(0.2, 0.3, 0.4, 0.5), 0.832),
            new ImmutablePair<>(Arrays.asList(0.2, 0.3, 0.4, 0.5, 0.12), 0.85216),
            new ImmutablePair<>(Arrays.asList(1.0, 1.0, 1.0), 1.0),
            new ImmutablePair<>(Arrays.asList(1.0, 1.0, 1.0, 1.0), 1.0),
            new ImmutablePair<>(Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0), 1.0),
            new ImmutablePair<>(Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), 1.0),
            new ImmutablePair<>(Arrays.asList(0.0, 0.0, 0.0), 0.0),
            new ImmutablePair<>(Arrays.asList(1.0), 1.0),
            new ImmutablePair<>(Arrays.asList(0.2), 0.2),
            new ImmutablePair<>(Arrays.asList(0.1, 0.9, 0.3), 0.937),
            new ImmutablePair<>(Arrays.asList( 0.9, 0.1, 0.3), 0.937),
            new ImmutablePair<>(Arrays.asList( 0.9, 0.3, 0.1 ), 0.937),
            new ImmutablePair<>(Arrays.asList(), 0.0));
    
    for (Pair<List<Double>, Double> pair : data) {
      assertEquals(pair.getRight(), ArithmeticUtils.getUnionProbability(pair.getLeft()), 0.001);
    }
  }

}
