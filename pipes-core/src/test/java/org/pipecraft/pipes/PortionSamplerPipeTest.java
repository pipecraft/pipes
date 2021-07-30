package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Random;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.sample.PortionSamplerPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * A test for the {@link PortionSamplerPipe}, which samples items of a given input pipe
 * 
 * @author Eyal Schneider
 */
public class PortionSamplerPipeTest {
  @Test
  public void testSampleAll() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (PortionSamplerPipe<Integer> p = new PortionSamplerPipe<>(p1, 1.0)) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testSampleNone() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (PortionSamplerPipe<Integer> p = new PortionSamplerPipe<>(p1, 0.0)) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testSample() throws Exception {
    final double fraction = 0.2;
    final int total = 10_000;
    SeqGenPipe<Integer> p1 = new SeqGenPipe<>(i -> (i < total) ? i.intValue() : null);
    try (PortionSamplerPipe<Integer> p = new PortionSamplerPipe<>(p1, fraction, new Random(112233))) { // Setting the random object for test consistency
      p.start();
      int sampledCount = 0;
      while (p.next() != null) {
        sampledCount++;
      }
      assertEquals(fraction, sampledCount / (double)total , 0.05); //5% error margin
    }
  }

}
