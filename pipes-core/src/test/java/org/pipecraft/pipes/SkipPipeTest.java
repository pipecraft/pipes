package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.SkipPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link SkipPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class SkipPipeTest {
  
  @Test
  public void testEmpty() throws Exception {
    try (
        SkipPipe<Integer> p = new SkipPipe<>(EmptyPipe.instance(), 10);
        ) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testIncludeAll() throws Exception {
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i, 3);
        SkipPipe<Long> p = new SkipPipe<>(p0, 0);
        ) {
      p.start();
      assertEquals(0, p.next().longValue());
      assertEquals(1, p.next().longValue());
      assertEquals(2, p.next().longValue());
      assertNull(p.next());
    }
  }

  @Test
  public void testIncludeSome() throws Exception {
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i, 3);
        SkipPipe<Long> p = new SkipPipe<>(p0, 1);
        ) {
      p.start();
      assertEquals(1, p.next().longValue());
      assertEquals(2, p.next().longValue());
      assertNull(p.next());
    }
  }

  @Test
  public void testSkipAll() throws Exception {
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i, 10);
        SkipPipe<Long> p = new SkipPipe<>(p0, 10);
        ) {
      p.start();
      assertNull(p.next());
    }
  }

}
