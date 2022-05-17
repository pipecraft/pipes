package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link CollectionWriterPipe}
 *
 * @author Eyal Schneider
 */
public class CollectionWriterPipeTest {

  @Test
  public void testEmpty() throws Exception {
    List<Long> out = new ArrayList<>();
    try (
        Pipe<Long> genPipe = EmptyPipe.instance();
        CollectionWriterPipe<Long> tp = new CollectionWriterPipe<>(genPipe, out)) {
      tp.start();
      assertEquals(Collections.emptyList(), tp.getItems());
    }

  }

  @Test
  public void testNonEmptyWithDefaultCollection() throws Exception {
    try (
        Pipe<Long> genPipe = new SeqGenPipe<>(Function.identity(), 3);
        CollectionWriterPipe<Long> tp = new CollectionWriterPipe<>(genPipe)) {
      tp.start();
      assertEquals(List.of(0L, 1L, 2L), tp.getItems());
    }
  }

  @Test
  public void testNonEmptyWithProvidedCollection() throws Exception {
    List<Long> out = new ArrayList<>();
    out.add(-1L);
    try (
        SeqGenPipe<Long> genPipe = new SeqGenPipe<>(Function.identity(), 3);
        CollectionWriterPipe<Long> tp = new CollectionWriterPipe<>(genPipe, out)) {
      tp.start();
      assertEquals(List.of(-1L, 0L, 1L, 2L), tp.getItems());
      assertSame(out, tp.getItems());
    }
  }

}
