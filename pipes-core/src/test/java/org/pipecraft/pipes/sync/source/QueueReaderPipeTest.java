package org.pipecraft.pipes.sync.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.exceptions.QueuePipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.utils.QueueItem;

public class QueueReaderPipeTest {
  private static final QueueItem<Integer> ERROR_MARKER = QueueItem.error(new IOException());

  @Test
  public void testSuccess() throws Exception {
    ArrayBlockingQueue<QueueItem<Integer>> queue = new ArrayBlockingQueue<>(10);
    queue.add(QueueItem.of(0));
    queue.add(QueueItem.of(1));
    queue.add(QueueItem.of(2));
    queue.add(QueueItem.end());
    
    try (
      Pipe<Integer> qrp = new QueueReaderPipe<>(queue);
      CollectionWriterPipe<Integer> wp = new CollectionWriterPipe<>(qrp)
    ) {
      wp.start();
      assertEquals(Lists.newArrayList(0, 1, 2), wp.getItems());
    }
  }
  
  @Test
  public void testError() throws Exception {
    ArrayBlockingQueue<QueueItem<Integer>> queue = new ArrayBlockingQueue<>(10);
    queue.add(QueueItem.of(0));
    queue.add(QueueItem.of(1));
    queue.add(QueueItem.of(2));
    queue.add(ERROR_MARKER);
    
    try (
      Pipe<Integer> qrp = new QueueReaderPipe<>(queue)
    ) {
      qrp.start();
      assertEquals(0, qrp.next());
      assertEquals(1, qrp.next());
      assertEquals(2, qrp.next());
      assertThrows(QueuePipeException.class, qrp::next);
    }
  }

  @Test
  public void testPeek() throws Exception {
    ArrayBlockingQueue<QueueItem<Integer>> queue = new ArrayBlockingQueue<>(10);
    queue.add(QueueItem.of(10));
    queue.add(QueueItem.end());

    try (Pipe<Integer> qrp = new QueueReaderPipe<>(queue)) {
      qrp.start();
      assertEquals(10, qrp.peek());
      assertEquals(10, qrp.next());
      assertNull(qrp.next());
    }
  }


}
