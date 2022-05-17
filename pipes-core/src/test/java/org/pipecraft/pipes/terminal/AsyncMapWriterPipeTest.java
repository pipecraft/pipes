package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * Tests {@link AsyncMapWriterPipe}
 *
 * @author Eyal Schneider
 */
public class AsyncMapWriterPipeTest {

  @Test
  public void testEmpty() throws Exception {
    Map<Integer, String> out = new ConcurrentHashMap<>();
    try (
        AsyncPipe<Long> genPipe = new AsyncEmptyPipe<>();
        AsyncMapWriterPipe<Long, Integer, String> tp = new AsyncMapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf, out)) {
      tp.start();
      assertEquals(Collections.emptyMap(), tp.getItems());
    }
  }

  @Test
  public void testNonEmptyWithDefaultCollection() throws Exception {
    try (
        AsyncPipe<Long> genPipe = new AsyncSeqGenPipe<>(4, Function.identity(), 2);
        AsyncMapWriterPipe<Long, Integer, String> tp = new AsyncMapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf)) {
      tp.start();
      assertEquals(Map.of(0, "0",  10, "1", 20, "2", 30, "3"), tp.getItems());
    }
  }

  @Test
  public void testNonEmptyWithProvidedCollection() throws Exception {
    Map<Integer, String> out = Collections.synchronizedMap(new HashMap<>());
    out.put(-10, "-1");
    try (
        AsyncPipe<Long> genPipe = new AsyncSeqGenPipe<>(4, Function.identity(), 2);
        AsyncMapWriterPipe<Long, Integer, String> tp = new AsyncMapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf, out)) {
      tp.start();
      assertEquals(Map.of(-10, "-1", 0, "0",  10, "1", 20, "2", 30, "3"), tp.getItems());
      assertSame(out, tp.getItems());
    }
  }

  @Test
  public void testValidationError() throws Exception {
    try (
        AsyncPipe<Long> genPipe = new AsyncSeqGenPipe<>(4, Function.identity(), 2);
        TerminalPipe tp = new AsyncMapWriterPipe<>(genPipe, p -> {
          if (p > 2) {
            throw new ValidationPipeException("Too big");
          }
          return p;
        }, p -> p)) {
      assertThrows(ValidationPipeException.class, tp::start);
    }
  }

}
