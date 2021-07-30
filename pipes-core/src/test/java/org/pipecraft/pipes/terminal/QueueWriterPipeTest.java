package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import org.pipecraft.pipes.AsyncTester;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests {@link QueueWriterPipe}
 *
 * @author Eyal Schneider
 */
public class QueueWriterPipeTest {
  private static final String SUCCESS_MARKER = new String("queue_success"); // Must be a unique reference
  private static final String ERROR_MARKER = new String("queue_error"); // Must be a unique reference

  @Test
  public void testEmpty() throws Exception {
    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    try (
        Pipe<String> p = EmptyPipe.instance();
        TerminalPipe tp = new QueueWriterPipe<>(p, queue, SUCCESS_MARKER, ERROR_MARKER)
    ) {
      tp.start();
      assertSame(SUCCESS_MARKER, queue.peek());
      assertEquals(1, queue.size());
    }
  }

  public void testFewItems() throws Exception {
    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    try (
        Pipe<String> p = new SeqGenPipe<>(Object::toString, 10);
        TerminalPipe tp = new QueueWriterPipe<>(p, queue, SUCCESS_MARKER, ERROR_MARKER)
    ) {
      tp.start();
      for(int i = 0; i < 10; i++) {
        String item = queue.poll(3, TimeUnit.SECONDS);
        assertEquals(String.valueOf(i), item);
      }
      assertSame(SUCCESS_MARKER, queue.peek());
      assertEquals(1, queue.size());
    }
  }

  @Test
  @Timeout(20)
  public void testBlocking() throws Exception {
    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(10);

    AsyncTester asyncTester = new AsyncTester(() -> {
      try {
        for (int i = 0; i < 100; i++) {
          String item = queue.poll(3, TimeUnit.SECONDS);
          if (i % 2 == 0) {
            Thread.sleep(1); // Delay reads intentionally
          }
          assertEquals(String.valueOf(i), item);
        }
        assertSame(SUCCESS_MARKER, queue.peek());
        assertEquals(1, queue.size());
      } catch (InterruptedException e) {
        fail("Got interrupted", e);
      }
    });
    asyncTester.start();

    try (
        Pipe<String> p = new SeqGenPipe<>(Object::toString, 100); // Trying to push 100 items when capacity=10, so blocking is expected if we don't pull too fast.
        TerminalPipe tp = new QueueWriterPipe<>(p, queue, SUCCESS_MARKER, ERROR_MARKER)
    ) {
      tp.start();
      asyncTester.test();
    }
  }

}
