package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests the different implementations of pipes performing data sharding
 * 
 * @author Eyal Schneider
 *
 */
public class SharderByItemPipeTest {

  @Test
  public void testSharderBySeqPipeEmpty() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        SharderBySeqPipe<Pair<String, Integer>> p = new SharderBySeqPipe<>(
            EmptyPipe.instance(), 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
    ) {
      p.start();
    }

    File[] shards = folder.listFiles();
    assertEquals(0, shards.length);
  }

  @Test
  public void testSharderBySeqPipeOneShard() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(Lists.newArrayList(
            new ImmutablePair<>("A", 3), 
            new ImmutablePair<>("B", 3),
            new ImmutablePair<>("C", 3)));
        SharderBySeqPipe<Pair<String, Integer>> p = new SharderBySeqPipe<>(
            p0, 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
    ) {
      p.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(1, p.getShardSizes().size());
    }

    File[] shards = folder.listFiles();
    assertEquals(1, shards.length);
    assertEquals(Lists.newArrayList(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3")));
  }
  
  @Test
  public void testSharderBySeqPipe() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(Lists.newArrayList(
            new ImmutablePair<>("A", 3), 
            new ImmutablePair<>("B", 3),
            new ImmutablePair<>("C", 3),
            new ImmutablePair<>("D", 2),
            new ImmutablePair<>("E", 1),
            new ImmutablePair<>("F", 1)));
        SharderBySeqPipe<Pair<String, Integer>> p = new SharderBySeqPipe<>(
            p0, 
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder
            );
    ) {
      p.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("2"));
      assertEquals(Integer.valueOf(2), p.getShardSizes().get("1"));
      assertEquals(3, p.getShardSizes().size());
    }

    File[] shards = folder.listFiles();
    assertEquals(3, shards.length);
    assertEquals(Lists.newArrayList(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3")));
    assertEquals(Lists.<List<String>>newArrayList(Collections.singletonList("D_2")), FileUtils.readCSV(new File(folder, "2")));
    assertEquals(Lists.newArrayList(Collections.singletonList("E_1"), Collections.singletonList("F_1")), FileUtils.readCSV(new File(folder, "1")));
  }

  @Test
  public void testSharderBySeqPipeCompressed() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Pair<String, Integer>> p0 = new CollectionReaderPipe<>(Lists.newArrayList(
            new ImmutablePair<>("A", 3), 
            new ImmutablePair<>("B", 3),
            new ImmutablePair<>("C", 3),
            new ImmutablePair<>("D", 2),
            new ImmutablePair<>("E", 1),
            new ImmutablePair<>("F", 1)));
        SharderBySeqPipe<Pair<String, Integer>> p = new SharderBySeqPipe<>(
            p0,
            new TxtEncoderFactory<>(pair -> pair.getKey() + "_" + pair.getValue(), StandardCharsets.UTF_8),
            pair -> pair.getValue().toString(),
            folder,
            new FileWriteOptions().setCompression(Compression.GZIP)
            );
    ) {
      p.start();
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("3"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("2"));
      assertEquals(Integer.valueOf(2), p.getShardSizes().get("1"));
      assertEquals(3, p.getShardSizes().size());
    }

    File[] shards = folder.listFiles();
    assertEquals(3, shards.length);
    assertEquals(Lists.newArrayList(Collections.singletonList("A_3"), Collections.singletonList("B_3"), Collections.singletonList("C_3")), FileUtils.readCSV(new File(folder, "3"), new FileReadOptions().setCompression(Compression.GZIP)));
    assertEquals(Lists.<List<String>>newArrayList(Collections.singletonList("D_2")), FileUtils.readCSV(new File(folder, "2"), new FileReadOptions().setCompression(Compression.GZIP)));
    assertEquals(Lists.newArrayList(Collections.singletonList("E_1"), Collections.singletonList("F_1")), FileUtils.readCSV(new File(folder, "1"), new FileReadOptions().setCompression(Compression.GZIP)));
  }

  @Test
  public void testSharderByItemPipeEmpty() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        SharderByItemPipe<Integer> p = new SharderByItemPipe<>(
            EmptyPipe.instance(),
            new TxtEncoderFactory<>(Object::toString, StandardCharsets.UTF_8),
            item -> String.valueOf(item % 10), // Buckets are defined by the unit digit of each number
            folder,
            new FileWriteOptions()
            );
    ) {
      p.start();
      assertEquals(0, p.getShardSizes().size());
    }

    File[] shards = folder.listFiles();
    assertEquals(0, shards.length);
  }

  @Test
  public void testSharderByItemPipeOneShard() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Integer> p0 = new CollectionReaderPipe<>(Lists.newArrayList(14, 24, 1004));
        SharderByItemPipe<Integer> p = new SharderByItemPipe<>(
            p0, 
            new TxtEncoderFactory<>(v -> v.toString(), StandardCharsets.UTF_8),
            item -> String.valueOf(item % 10), // Buckets are defined by the unit digit of each number
            folder,
            new FileWriteOptions()
            );
    ) {
      p.start();
      assertEquals(1, p.getShardSizes().size());
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("4"));
    }

    File[] shards = folder.listFiles();
    assertEquals(1, shards.length);
    assertEquals(Lists.newArrayList(Collections.singletonList("14"), Collections.singletonList("24"), Collections.singletonList("1004")), FileUtils.readCSV(new File(folder, "4")));
  }

  @Test
  public void testSharderByItemPipe() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    
    try (
        CollectionReaderPipe<Integer> p0 = new CollectionReaderPipe<>(Lists.newArrayList(11, 20, 14, 30, 21, 31, 35));
        SharderByItemPipe<Integer> p = new SharderByItemPipe<>(
            p0, 
            new TxtEncoderFactory<>(v -> v.toString()),
            item -> String.valueOf(item % 10), // Buckets are defined by the unit digit of each number
            folder,
            new FileWriteOptions()
            );
    ) {
      p.start();
      assertEquals(4, p.getShardSizes().size());
      assertEquals(Integer.valueOf(2), p.getShardSizes().get("0"));
      assertEquals(Integer.valueOf(3), p.getShardSizes().get("1"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("4"));
      assertEquals(Integer.valueOf(1), p.getShardSizes().get("5"));
    }

    File[] shards = folder.listFiles();
    assertEquals(4, shards.length);
    assertEquals(Lists.newArrayList(Collections.singletonList("20"), Collections.singletonList("30")), FileUtils.readCSV(new File(folder, "0")));
    assertEquals(Lists.newArrayList(Collections.singletonList("11"), Collections.singletonList("21"), Collections.singletonList("31")), FileUtils.readCSV(new File(folder, "1")));
    assertEquals(Lists.<List<String>>newArrayList(Collections.singletonList("14")), FileUtils.readCSV(new File(folder, "4")));
    assertEquals(Lists.<List<String>>newArrayList(Collections.singletonList("35")), FileUtils.readCSV(new File(folder, "5")));
  }
  
  @Test
  public void testSharderByHashPipe() throws Exception {
    File folder = FileUtils.createTempFolder(SharderBySeqPipe.class.getSimpleName());
    final int SHARDS = 10;
    final int TOTAL_ITEMS = 100_000;
    Random rnd = new Random(223344);
    try (
        SeqGenPipe<String> p0 = new SeqGenPipe<>(i -> String.valueOf(rnd.nextInt()), TOTAL_ITEMS);
        SharderByHashPipe<String> p = new SharderByHashPipe<>(
            p0, 
            new TxtEncoderFactory<>(Functions.identity(), StandardCharsets.UTF_8),
            FailableFunction.identity(),
            SHARDS,
            folder,
            new FileWriteOptions()
            );
    ) {
      p.start();
      assertEquals(10, p.getShardSizes().size());
      int expectedPerShard = TOTAL_ITEMS / SHARDS;
      for (int shardSize : p.getShardSizes().values()) {
        assertTrue(Math.abs(shardSize - expectedPerShard) / (float)expectedPerShard < 0.05); // Less than 5% deviation from expected bucket size
      }
    }
  }
}
