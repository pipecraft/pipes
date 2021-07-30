package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.serialization.TxtCodecFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link DedupPipe}
 */
public class DedupPipeTest {

  @Test
  public void testEmpty() throws Exception {
    try (
        Pipe<String> srcP = EmptyPipe.instance();
        Pipe<String> dedupP = new DedupPipe<>(srcP,
            new TxtCodecFactory<>(Function.identity(), FailableFunction.identity()), 10,
            FileUtils.getSystemDefaultTmpFolder());
    ) {
      dedupP.start();
      assertNull(dedupP.next());
    }
  }

  @Test
  public void testDedup() throws Exception {
    List<String> list = new ArrayList<>();
    try (
        Pipe<String> srcP = new CollectionReaderPipe<>(
            List.of("a", "c", "c", "A", "a", "b", "c", "a", "A"));
        Pipe<String> dedupP = new DedupPipe<>(srcP,
            new TxtCodecFactory<>(Function.identity(), FailableFunction.identity()), 10,
            FileUtils.getSystemDefaultTmpFolder());
        TerminalPipe tp = new CollectionWriterPipe<>(dedupP, list);
    ) {
      tp.start();
      assertEquals(Set.of("a", "A", "b", "c"), new HashSet<>(list));
      assertEquals(4, list.size());
    }
  }

  @Test
  public void testSingleRepeatedValue() throws Exception {
    List<String> list = new ArrayList<>();
    try (
        Pipe<String> srcP = new CollectionReaderPipe<>(List.of("a", "a", "a"));
        Pipe<String> dedupP = new DedupPipe<>(srcP,
            new TxtCodecFactory<>(Function.identity(), FailableFunction.identity()), 10,
            FileUtils.getSystemDefaultTmpFolder());
        TerminalPipe tp = new CollectionWriterPipe<>(dedupP, list);
    ) {
      tp.start();
      assertEquals(List.of("a"), list);
    }
  }
}