package org.pipecraft.pipes.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

/**
 * Tests {@link PipeUtils}
 * 
 * @author Eyal Schneider
 */
public class PipeUtilsTest {
  @Test
  public void testNoTransformation() {
    test(0, 100, 1,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 100),
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 100));
  }
  
  @Test
  public void testStepVer1() {
    test(0, 100, 10,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 95, 100),
        Lists.newArrayList(0, 20, 50, 95, 100));
  }

  @Test
  public void testStepVer2() {
    test(0, 100, 30,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 95, 100),
        Lists.newArrayList(0, 50, 95, 100));
  }

  @Test
  public void testScale() {
    test(0, 20, 1,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 60, 95, 100),
        Lists.newArrayList(0, 4, 5, 10, 12, 19, 20));
  }

  @Test
  public void testScaleAndMove() {
    test(50, 60, 1,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 60, 95, 100),
        Lists.newArrayList(50, 52, 55, 56, 59, 60));
  }

  @Test
  public void testScaleAndMoveAndStep() {
    test(30, 80, 10,
        Lists.newArrayList(0, 1, 2, 20, 25, 50, 60, 95, 100),
        Lists.newArrayList(30, 40, 55, 77, 80));
  }

  private void test(int from, int to, int step, List<Integer> inputPcts, List<Integer> expectedOutput) {
    List<Integer> reported = new ArrayList<>();
    Consumer<Integer> l = reported::add;
    l = new ProgressTransformer(l, from, to, step);
    for (Integer pct : inputPcts) {
      l.accept(pct);
    }
    assertEquals(expectedOutput, reported);
  }

}
