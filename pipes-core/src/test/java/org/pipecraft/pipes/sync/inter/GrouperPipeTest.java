package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.sync.inter.reduct.ListReductorPipe;
import org.pipecraft.pipes.serialization.TxtCodecFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link GrouperPipe}
 */
public class GrouperPipeTest {

  @Test
  public void testEmpty() throws Exception {
    try (
        Pipe<String> inputP = EmptyPipe.instance();
        Pipe<String> grouperP = new GrouperPipe<>(inputP, String::length,
            new TxtCodecFactory<>(x -> x, x -> x), 10, FileUtils.getSystemDefaultTmpFolder());
    ) {
      grouperP.start();
      assertNull(grouperP.next());
    }
  }

  @Test
  public void testGroupByStrLength() throws Exception {
    List<List<String>> out = new ArrayList<>();
    try (
      Pipe<String> inputP = new CollectionReaderPipe<>(Arrays.asList("what", "be", "a", "bee", "all", "where", "ball"));
      Pipe<String> grouperP = new GrouperPipe<>(inputP, String::length, new TxtCodecFactory<>(x -> x, x -> x), 10, FileUtils.getSystemDefaultTmpFolder());
      Pipe<List<String>> listP = new ListReductorPipe<>(grouperP, String::length, Function.identity());
      TerminalPipe tp = new CollectionWriterPipe<>(listP, out);
        ) {
      tp.start();
      Collections.sort(out, Comparator.comparing(l -> l.get(0).length()));
      assertEquals(List.of(
          List.of("a"),
          List.of("be"),
          List.of("bee", "all"),
          List.of("what", "ball"),
          List.of("where")
      ), out);
    }
  }

  @Test
  public void testSingleFamily() throws Exception {
    List<List<String>> out = new ArrayList<>();
    try (
        Pipe<String> inputP = new CollectionReaderPipe<>(Arrays.asList("an", "be", "or", "to"));
        Pipe<String> grouperP = new GrouperPipe<>(inputP, String::length, new TxtCodecFactory<>(x -> x, x -> x), 10, FileUtils.getSystemDefaultTmpFolder());
        Pipe<List<String>> listP = new ListReductorPipe<>(grouperP, String::length, Function.identity());
        TerminalPipe tp = new CollectionWriterPipe<>(listP, out);
    ) {
      tp.start();
      Collections.sort(out, Comparator.comparing(l -> l.get(0).length()));
      assertEquals(List.of(
          List.of("an", "be", "or", "to")
      ), out);
    }
  }

}
