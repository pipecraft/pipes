package org.pipecraft.pipes.sync.inter.reduct;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.sync.inter.SortPipe;
import org.pipecraft.pipes.serialization.TxtCodecFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link HashReductorPipe}
 */
public class HashReductorPipeTest {

  @Test
  public void testEmpty() throws Exception {
    List<String> res = new ArrayList<>();
    try (
        Pipe<String> ep = EmptyPipe.instance();
        Pipe<String> rp = new HashReductorPipe<>(ep, new TxtCodecFactory<>(x -> x, x -> x), 10, FileUtils
            .getSystemDefaultTmpFolder(),
            ReductorConfig.<String, String, Pair<String, MutableInt>, String>builder()
                .discriminator(x -> x)
                .aggregatorCreator(x -> new ImmutablePair<>(x, new MutableInt()))
                .aggregationLogic((pair, v) -> pair.getRight().increment())
                .postProcessor(pair -> pair.getLeft() + "_" + pair.getRight()).build());
        TerminalPipe tp = new CollectionWriterPipe<>(rp, res);
        ) {
      tp.start();
      assertEquals(0, res.size());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 5, 10})
  public void testWordCount(int partitionCount) throws Exception {
    List<String> res = new ArrayList<>();
    try (
        Pipe<String> ep = new CollectionReaderPipe<>(
            Arrays.asList("why", "who", "what", "when", "why", "what", "what"));
        Pipe<String> rp = new HashReductorPipe<>(ep, new TxtCodecFactory<>(x -> x, x -> x), partitionCount, FileUtils.getSystemDefaultTmpFolder(),
            ReductorConfig.<String, String, Pair<String, MutableInt>, String>builder()
            .discriminator(x -> x)
            .aggregatorCreator(x -> new ImmutablePair<>(x, new MutableInt()))
            .aggregationLogic((pair, v) -> pair.getRight().increment())
            .postProcessor(pair -> pair.getLeft() + "_" + pair.getRight()).build());
        SortPipe<String> sp = new SortPipe<>(rp, String::compareTo);
        TerminalPipe tp = new CollectionWriterPipe<>(sp, res);
    ) {
      tp.start();
      assertEquals(Arrays.asList("what_3", "when_1", "who_1", "why_2"), res);
    }
  }

  @Test
  public void testSingleFamily() throws Exception {
    List<String> res = new ArrayList<>();
    try (
        Pipe<String> ep = new CollectionReaderPipe<>(
            Arrays.asList("what", "what"));
        Pipe<String> rp = new HashReductorPipe<>(ep, new TxtCodecFactory<>(x -> x, x -> x), 10, FileUtils.getSystemDefaultTmpFolder(),
            ReductorConfig.<String, String, Pair<String, MutableInt>, String>builder()
                .discriminator(x -> x)
                .aggregatorCreator(x -> new ImmutablePair<>(x, new MutableInt()))
                .aggregationLogic((pair, v) -> pair.getRight().increment())
                .postProcessor(pair -> pair.getLeft() + "_" + pair.getRight()).build());
        SortPipe<String> sp = new SortPipe<>(rp, String::compareTo);
        TerminalPipe tp = new CollectionWriterPipe<>(sp, res);
    ) {
      tp.start();
      assertEquals(Arrays.asList("what_2"), res);
    }
  }

}
