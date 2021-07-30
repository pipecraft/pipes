package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.SharderBySeqPipe;
import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link IntermediateSharderBySeqPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class IntermediateSharderBySeqPipeTest {

  @Test
  public void testEmpty() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        IntermediateSharderBySeqPipe<Pair<String, Integer>> p = new IntermediateSharderBySeqPipe<>(
            EmptyPipe.instance(), 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
        CollectionWriterPipe<?> tp = new CollectionWriterPipe<>(p);
    ) {
      tp.start();
      assertEquals(0, tp.getItems().size());
    }

    File[] shards = folder.listFiles();
    assertEquals(0, shards.length);
  }

  @Test
  public void testOneShard() throws Exception {
    List<Pair<String, Integer>> items = List.of(
        new ImmutablePair<>("A", 3),
        new ImmutablePair<>("B", 3),
        new ImmutablePair<>("C", 3));
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(items);
        IntermediateSharderBySeqPipe<Pair<String, Integer>> p = new IntermediateSharderBySeqPipe<>(
            p0, 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
        CollectionWriterPipe<Pair<String, Integer>> tp = new CollectionWriterPipe<>(p);
    ) {
      tp.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(1, p.getShardSizes().size());
      assertEquals(items, tp.getItems());
    }

    File[] shards = folder.listFiles();
    assertEquals(1, shards.length);
    assertEquals(List.of(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3")));
  }
  
  @Test
  public void testGeneralCase() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    List<Pair<String, Integer>> items = List.of(
        new ImmutablePair<>("A", 3),
        new ImmutablePair<>("B", 3),
        new ImmutablePair<>("C", 3),
        new ImmutablePair<>("D", 2),
        new ImmutablePair<>("E", 1),
        new ImmutablePair<>("F", 1));
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(items);
        IntermediateSharderBySeqPipe<Pair<String, Integer>> p = new IntermediateSharderBySeqPipe<>(
            p0, 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
        CollectionWriterPipe<Pair<String, Integer>> tp = new CollectionWriterPipe<>(p);
    ) {
      tp.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("2"));
      assertEquals(Integer.valueOf(2), p.getShardSizes().get("1"));
      assertEquals(3, p.getShardSizes().size());
      assertEquals(items, tp.getItems());
    }

    File[] shards = folder.listFiles();
    assertEquals(3, shards.length);
    assertEquals(List.of(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3")));
    assertEquals(List.of(Collections.singletonList("D_2")), FileUtils.readCSV(new File(folder, "2")));
    assertEquals(List.of(Collections.singletonList("E_1"), Collections.singletonList("F_1")), FileUtils.readCSV(new File(folder, "1")));
  }

  @Test
  public void testCompressed() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());

    List<Pair<String, Integer>> items = List.of(
        new ImmutablePair<>("A", 3),
        new ImmutablePair<>("B", 3),
        new ImmutablePair<>("C", 3),
        new ImmutablePair<>("D", 2),
        new ImmutablePair<>("E", 1),
        new ImmutablePair<>("F", 1));
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(items);
        IntermediateSharderBySeqPipe<Pair<String, Integer>> p = new IntermediateSharderBySeqPipe<>(
            p0,
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder,
            new FileWriteOptions().setCompression(Compression.GZIP)
            );
        CollectionWriterPipe<Pair<String, Integer>> tp = new CollectionWriterPipe<>(p);
    ) {
      tp.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("2"));
      assertEquals(Integer.valueOf(2), p.getShardSizes().get("1"));
      assertEquals(3, p.getShardSizes().size());
      assertEquals(items, tp.getItems());
    }

    File[] shards = folder.listFiles();
    assertEquals(3, shards.length);
    assertEquals(List.of(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3"), new FileReadOptions().setCompression(Compression.GZIP)));
    assertEquals(List.of(Collections.singletonList("D_2")), FileUtils.readCSV(new File(folder, "2"), new FileReadOptions().setCompression(Compression.GZIP)));
    assertEquals(List.of(Collections.singletonList("E_1"), Collections.singletonList("F_1")), FileUtils.readCSV(new File(folder, "1"), new FileReadOptions().setCompression(Compression.GZIP)));
  }
}
