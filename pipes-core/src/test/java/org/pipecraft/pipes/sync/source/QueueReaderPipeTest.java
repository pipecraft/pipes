package org.pipecraft.pipes.sync.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.exceptions.QueuePipeException;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;

public class QueueReaderPipeTest {
  private static final Integer SUCCESS_MARKER = new Integer(0);
  private static final Integer ERROR_MARKER = new Integer(0);

  @Test
  public void testSuccess() throws Exception {
    ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
    queue.add(0);
    queue.add(1);
    queue.add(2);
    queue.add(SUCCESS_MARKER);
    
    try (
      QueueReaderPipe<Integer> qrp = new QueueReaderPipe<Integer>(queue, SUCCESS_MARKER, ERROR_MARKER);
      CollectionWriterPipe<Integer> wp = new CollectionWriterPipe<>(qrp);
    ) {
      wp.start();
      assertEquals(Lists.newArrayList(0, 1, 2), wp.getItems());
      
    }
  }
  
  @Test
  public void testError() throws Exception {
    ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
    queue.add(0);
    queue.add(1);
    queue.add(2);
    queue.add(ERROR_MARKER);
    
    try (
      QueueReaderPipe<Integer> qrp = new QueueReaderPipe<Integer>(queue, SUCCESS_MARKER, ERROR_MARKER);
    ) {
      qrp.start();
      assertEquals(0, qrp.next());
      assertEquals(1, qrp.next());
      assertEquals(2, qrp.next());
      assertThrows(QueuePipeException.class, () -> qrp.next());
    }
  }

}
