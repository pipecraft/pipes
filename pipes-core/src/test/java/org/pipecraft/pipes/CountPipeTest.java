package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.google.common.base.Functions;
import org.pipecraft.pipes.sync.inter.CountPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * A test for the {@link CountPipe}, which counts items in the input pipe
 * 
 * @author Eyal Schneider
 */
public class CountPipeTest {
  @Test
  public void testEmpty() throws Exception {
    try (CountPipe p = new CountPipe(EmptyPipe.instance())) {
      p.start();
      assertEquals(Integer.valueOf(0), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testNonEmpty() throws Exception {
    SeqGenPipe<Long> p1 = new SeqGenPipe<>(Functions.identity(), 155);
    try (CountPipe p = new CountPipe(p1)) {
      p.start();
      assertEquals(Integer.valueOf(155), p.next());
      assertNull(p.next());
    }
  }
}