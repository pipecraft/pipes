package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.pipecraft.pipes.AsyncTester;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.pipecraft.pipes.utils.QueueItem;

/**
 * Tests {@link QueueWriterPipe}
 *
 * @author Eyal Schneider
 */
public class QueueWriterPipeTest {

  @Test
  public void testEmpty() throws Exception {
    LinkedBlockingQueue<QueueItem<String>> queue = new LinkedBlockingQueue<>();
    try (
        Pipe<String> p = EmptyPipe.instance();
        TerminalPipe tp = new QueueWriterPipe<>(p, queue)
    ) {
      tp.start();
      assertTrue(queue.peek().isSuccessfulEndOfData());
      assertEquals(1, queue.size());
    }
  }

  @Test
  public void testFewItems() throws Exception {
    LinkedBlockingQueue<QueueItem<String>> queue = new LinkedBlockingQueue<>();
    try (
        Pipe<String> p = new SeqGenPipe<>(Object::toString, 10);
        TerminalPipe tp = new QueueWriterPipe<>(p, queue)
    ) {
      tp.start();
      for(int i = 0; i < 10; i++) {
        QueueItem<String> itemW = queue.poll(3, TimeUnit.SECONDS);
        assertEquals(String.valueOf(i), itemW.getItem());
      }
      assertSame(QueueItem.end(), queue.peek());
      assertEquals(1, queue.size());
    }
  }

  @Test
  @Timeout(20)
  public void testBlocking() throws Exception {
    LinkedBlockingQueue<QueueItem<String>> queue = new LinkedBlockingQueue<>(10);

    AsyncTester asyncTester = new AsyncTester(() -> {
      try {
        for (int i = 0; i < 100; i++) {
          QueueItem<String> itemW = queue.poll(3, TimeUnit.SECONDS);
          if (i % 2 == 0) {
            Thread.sleep(1); // Delay reads intentionally
          }
          assertEquals(String.valueOf(i), itemW.getItem());
        }
        assertTrue(queue.peek().isSuccessfulEndOfData());
        assertEquals(1, queue.size());
      } catch (InterruptedException e) {
        fail("Got interrupted", e);
      }
    });
    asyncTester.start();

    try (
        Pipe<String> p = new SeqGenPipe<>(Object::toString, 100); // Trying to push 100 items when capacity=10, so blocking is expected if we don't pull too fast.
        TerminalPipe tp = new QueueWriterPipe<>(p, queue)
    ) {
      tp.start();
      asyncTester.test();
    }
  }

}
