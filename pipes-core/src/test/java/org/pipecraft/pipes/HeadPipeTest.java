package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.HeadPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link HeadPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class HeadPipeTest {
  
  @Test
  public void testEmptyInput() throws Exception {
    try (
        HeadPipe<Integer> p = new HeadPipe<>(EmptyPipe.instance(), 10);
        ) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testHead0() throws Exception {
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i, 3);
        HeadPipe<Long> p = new HeadPipe<>(p0, 0);
        ) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testIncludeAll() throws Exception {
    try (
        SeqGenPipe<Long> p0 = new SeqGenPipe<>(i -> i, 3);
        HeadPipe<Long> p = new HeadPipe<>(p0, 10);
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
        HeadPipe<Long> p = new HeadPipe<>(p0, 2);
        ) {
      p.start();
      assertEquals(0, p.next().longValue());
      assertEquals(1, p.next().longValue());
      assertNull(p.next());
    }
  }

}
