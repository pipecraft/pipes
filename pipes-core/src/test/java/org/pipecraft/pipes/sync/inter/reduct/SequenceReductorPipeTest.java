package org.pipecraft.pipes.sync.inter.reduct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SequenceReductorPipe}
 * 
 * @author Eyal Schneider
 */
public class SequenceReductorPipeTest {
  @Test
  public void testEmpty() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>();
        SequenceReductorPipe<Integer, Integer> rp = new SequenceReductorPipe<>(lp,
            ReductorConfig.<Integer, Integer, MutableInt, Integer>builder()
                .discriminator(FailableFunction.identity())
                .aggregatorCreator(f -> new MutableInt())
                .aggregationLogic((counter, v) -> counter.increment())
                .postProcessor(MutableInt::intValue).build());
        ) {
      rp.start();
      assertNull(rp.next());
    }
  }
  
  @Test
  public void testNoRepetitions() throws Exception {
    try (
        CollectionReaderPipe<Integer> lp = new CollectionReaderPipe<>(1, 2, 4, 8);
        SequenceReductorPipe<Integer, Integer> rp = new SequenceReductorPipe<>(lp,
            ReductorConfig.<Integer, Integer, MutableInt, Integer>builder()
                .discriminator(FailableFunction.identity())
                .aggregatorCreator(f -> new MutableInt())
                .aggregationLogic((counter, v) -> counter.increment())
                .postProcessor(MutableInt::intValue).build());
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
        SequenceReductorPipe<Integer, Integer> rp = new SequenceReductorPipe<>(sg,
            ReductorConfig.<Integer, Integer, MutableInt, Integer>builder()
                .discriminator(FailableFunction.identity())
                .aggregatorCreator(f -> new MutableInt())
                .aggregationLogic((counter, v) -> counter.increment())
                .postProcessor(MutableInt::intValue).build());
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
        SequenceReductorPipe<Integer, Integer> rp = new SequenceReductorPipe<>(lp,
            ReductorConfig.<Integer, Integer, MutableInt, Integer>builder()
                .discriminator(FailableFunction.identity())
                .aggregatorCreator(f -> new MutableInt())
                .aggregationLogic((counter, v) -> counter.increment())
                .postProcessor(MutableInt::intValue).build());
        ) {
      rp.start();        
      assertEquals(Integer.valueOf(2), rp.next());
      assertEquals(Integer.valueOf(3), rp.next());
      assertEquals(Integer.valueOf(4), rp.next());
      assertEquals(Integer.valueOf(5), rp.next());
      assertNull(rp.next());
    }
  }

}
