package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.inter.CallbackPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;

/**
 * A test for the {@link CallbackPipe}, which supports item level and completion level callbacks
 * 
 * @author Eyal Schneider
 */
public class CallbackPipeTest {
  @Test
  public void testItemCallback() throws Exception {
    MutableInt last = new MutableInt(-1);
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(1, 2, 3);
    try (CallbackPipe<Integer> p = new CallbackPipe<>(p1, v -> {last.setValue(v);})) {
      p.start();
      assertEquals(Integer.valueOf(-1), last.getValue());
      p.next();
      assertEquals(Integer.valueOf(1), last.getValue());
      p.next();
      assertEquals(Integer.valueOf(2), last.getValue());
      p.next();
      assertEquals(Integer.valueOf(3), last.getValue());
      assertNull(p.next());
      assertEquals(Integer.valueOf(3), last.getValue());
    }
  }
  
  @Test
  public void testTerminationCallback() throws Exception {
    MutableInt last = new MutableInt(-1);
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(1, 2, 3);
    try (CallbackPipe<Integer> p = new CallbackPipe<>(p1, () -> last.increment())) {
      p.start();
      assertEquals(Integer.valueOf(-1), last.getValue());
      
      for (int i = 0; i < 3; i++) {
        p.next();
        assertEquals(Integer.valueOf(-1), last.getValue());
      }

      p.next();
      assertEquals(Integer.valueOf(0), last.getValue());
      p.next();
      assertEquals(Integer.valueOf(0), last.getValue()); // Termination callback should not be executed more than once
    }
  }  

  @Test
  public void testTerminationCallbackThroughPeek() throws Exception {
    MutableInt last = new MutableInt(-1);
    EmptyPipe<Integer> p1 = EmptyPipe.instance();
    try (CallbackPipe<Integer> p = new CallbackPipe<>(p1, () -> last.increment())) {
      p.start();
      assertEquals(Integer.valueOf(-1), last.getValue());
      p.peek();
      assertEquals(Integer.valueOf(0), last.getValue());
    }
  }  

}
