package org.pipecraft.pipes.sync.inter.reduct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.pipecraft.infra.concurrent.FailableFunction;
import java.util.List;
import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.exceptions.ExcessiveResourcesUsagePipeException;
import org.pipecraft.pipes.sync.inter.reduct.ListReductorPipe.GroupSizeLimitPolicy;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link ListReductorPipe}
 * 
 * @author Eyal Schneider
 */
public class ListReductorPipeTest {
  @Test
  public void testEmpty() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>();
        ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(lp, FailableFunction.identity(), List::size);
        ) {
      rp.start();
      assertNull(rp.next());
    }
  }
  
  @Test
  public void testNoRepetitions() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>(1, 2, 4, 8);
        ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(lp, FailableFunction.identity(), List::size);
        ) {
      rp.start();
      for (int i = 0; i < 4; i++) {
        assertEquals(Integer.valueOf(1), rp.next());
      }
      assertNull(rp.next());
    }
  }

  @Test
  public void testSingleRepeatedValue() throws Exception {
    try (
        SeqGenPipe<Integer> sg = new SeqGenPipe<>(i -> 1, 100);
        ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(sg, FailableFunction.identity(), List::size);
        ) {
      rp.start();        
      assertEquals(Integer.valueOf(100), rp.next());
      assertNull(rp.next());
    }
  }

  @Test
  public void testGeneralCase() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>(1, 1, 2, 2, 2, 4, 4, 4, 4, 8, 8 ,8 ,8, 8);
        ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(lp, FailableFunction.identity(), List::size);
        ) {
      rp.start();        
      assertEquals(Integer.valueOf(2), rp.next());
      assertEquals(Integer.valueOf(3), rp.next());
      assertEquals(Integer.valueOf(4), rp.next());
      assertEquals(Integer.valueOf(5), rp.next());
      assertNull(rp.next());
    }
  }

  @Test
  public void testListTooLongTruncate() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>(1, 1, 2, 2, 2, 4, 4, 4, 4, 8, 8 ,8 ,8, 8);
        ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(lp, FailableFunction.identity(), List::size, 3, GroupSizeLimitPolicy.TRUNCATE);
        ) {
      rp.start();        
      assertEquals(Integer.valueOf(2), rp.next());
      assertEquals(Integer.valueOf(3), rp.next());
      assertEquals(Integer.valueOf(3), rp.next());
      assertEquals(Integer.valueOf(3), rp.next());
      assertNull(rp.next());
    }
  }

  @Test
  public void testListTooLongFail() throws Exception {
    assertThrows(ExcessiveResourcesUsagePipeException.class, () -> {
      try (
          CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>(1, 1, 2, 2, 2, 4, 4, 4, 4, 8, 8 ,8 ,8, 8);
          ListReductorPipe<Integer, Integer> rp = new ListReductorPipe<>(lp, FailableFunction.identity(), List::size, 3, GroupSizeLimitPolicy.FAIL);
          ) {
        rp.start();        
        assertEquals(Integer.valueOf(2), rp.next());
        assertEquals(Integer.valueOf(3), rp.next());
        rp.next();
      }
    });
  }
}
