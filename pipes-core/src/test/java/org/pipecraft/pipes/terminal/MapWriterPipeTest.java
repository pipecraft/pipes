package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link MapWriterPipe}
 *
 * @author Eyal Schneider
 */
public class MapWriterPipeTest {

  @Test
  public void testEmpty() throws Exception {
    Map<Integer, String> out = new HashMap<>();
    try (
        Pipe<Long> genPipe = EmptyPipe.instance();
        MapWriterPipe<Long, Integer, String> tp = new MapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf, out)) {
      tp.start();
      assertEquals(Collections.emptyMap(), tp.getItems());
    }
  }

  @Test
  public void testNonEmptyWithDefaultCollection() throws Exception {
    try (
        Pipe<Long> genPipe = new SeqGenPipe<>(Function.identity(), 3);
        MapWriterPipe<Long, Integer, String> tp = new MapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf)) {
      tp.start();
      assertEquals(Map.of(0, "0",  10, "1", 20, "2"), tp.getItems());
    }
  }

  @Test
  public void testNonEmptyWithProvidedCollection() throws Exception {
    Map<Integer, String> out = new HashMap<>();
    out.put(-10, "-1");
    try (
        Pipe<Long> genPipe = new SeqGenPipe<>(Function.identity(), 3);
        MapWriterPipe<Long, Integer, String> tp = new MapWriterPipe<>(genPipe, p -> 10 * p.intValue(), String::valueOf, out)) {
      tp.start();
      assertEquals(Map.of(-10, "-1", 0, "0",  10, "1", 20, "2"), tp.getItems());
      assertSame(out, tp.getItems());
    }
  }

  @Test
  public void testValidationError() throws Exception {
    try (
        Pipe<String> genPipe = new CollectionReaderPipe<>("1", "2", "3i");
        TerminalPipe tp = new MapWriterPipe<>(genPipe, p -> {
          try {
            return Integer.parseInt(p);
          } catch (NumberFormatException e) {
            throw new ValidationPipeException(e);
          }
        }, p -> p)) {
      assertThrows(ValidationPipeException.class, tp::start);
    }
  }

}
