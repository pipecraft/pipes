package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.pipecraft.pipes.exceptions.QueuePipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.ErrorPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

public class QueuePipeTest {

  @Test
  public void testEmpty() throws Exception {
    try (
        Pipe<Integer> emptyP = EmptyPipe.instance();
        Pipe<Integer> queueP = new QueuePipe<>(emptyP, 10)
        ) {
      queueP.start();

      assertNull(queueP.next());
    }
  }

  @Test
  public void testSuccess() throws Exception {
    try (
        Pipe<Integer> sourceP = new SeqGenPipe<>(Long::intValue, 100);
        Pipe<Integer> queueP = new QueuePipe<>(sourceP, 10)
    ) {
      queueP.start();

      for(int i = 0; i <= 99; i++) {
        assertEquals(i, queueP.next());
      }
      assertNull(queueP.next());
    }
  }

  @Test
  public void testEarlyClose() throws Exception {
    try (
        Pipe<Integer> sourceP = new SeqGenPipe<>(Long::intValue, 100);
        Pipe<Integer> queueP = new QueuePipe<>(sourceP, 10)
    ) {
      queueP.start();

      for(int i = 0; i <= 20; i++) {
        assertEquals(i, queueP.next());
      }
    } // queueP is closed prior to consuming all items
  }

  @Test
  public void testProducerError() throws Exception {
    try (
        Pipe<Integer> errP = new ErrorPipe<>(new QueuePipeException("Test error"));
        Pipe<Integer> sourceP = new SeqGenPipe<>(Long::intValue, 20);
        Pipe<Integer> concatP = new ConcatPipe<>(() -> sourceP, () -> errP);
        Pipe<Integer> queueP = new QueuePipe<>(concatP, 10)
    ) {
      queueP.start();

      assertThrows(QueuePipeException.class, () -> {
        for (int i = 0; i <= 20; i++) {
          assertEquals(i, queueP.next());
        }
        queueP.next();
      });
    }
  }

}
