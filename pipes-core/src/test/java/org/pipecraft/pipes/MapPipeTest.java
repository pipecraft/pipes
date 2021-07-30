package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.pipecraft.infra.concurrent.FailableFunction;
import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.MapPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * A test for the MapPipe, which transforms items
 * 
 * @author Eyal Schneider
 */
public class MapPipeTest {
  @Test
  public void testConvert() throws Exception {
    CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>("2", "3", "4");
    try (MapPipe<String, Integer> p = new MapPipe<>(p1, new ParseIntFunction())) {
      p.start();
      assertEquals(Integer.valueOf(2), p.next());
      assertEquals(Integer.valueOf(3), p.next());
      assertEquals(Integer.valueOf(4), p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testConvertEmpty() throws Exception {
    CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>();
    try (MapPipe<String, Integer> p = new MapPipe<>(p1, new ParseIntFunction())) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testConvertError() {
    assertThrows(IllegalNumberPipeException.class, () -> {
      CollectionReaderPipe<String> p1 = new CollectionReaderPipe<>("2", "3", "4","5e");
      try (MapPipe<String, Integer> p = new MapPipe<>(p1, new ParseIntFunction())) {
        p.start();
        while (p.next() != null);
      }
    });
  }

  private static class ParseIntFunction implements FailableFunction<String, Integer, PipeException> {

    @Override
    public Integer apply(String item) throws PipeException {
      try {
        return Integer.parseInt(item);
      } catch(NumberFormatException e) {
        throw new IllegalNumberPipeException(e);
      }
    }
  }
  
  @SuppressWarnings("serial")
  private static class IllegalNumberPipeException extends PipeException {
    public IllegalNumberPipeException(Throwable cause) {
      super(cause);
    }
  }
}