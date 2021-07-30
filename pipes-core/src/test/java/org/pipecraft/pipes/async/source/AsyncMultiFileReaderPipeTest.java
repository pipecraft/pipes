package org.pipecraft.pipes.async.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.Sets;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.MapPipe;
import org.pipecraft.pipes.serialization.TxtDecoderFactory;
import org.pipecraft.pipes.sync.source.TxtFileReaderPipe;
import org.pipecraft.pipes.terminal.AsyncCollectionWriterPipe;
import org.pipecraft.pipes.terminal.AsyncConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import org.pipecraft.pipes.utils.PipeReaderSupplier;
import org.pipecraft.pipes.utils.ShardSpecifier;
import org.pipecraft.pipes.utils.multi.LocalMultiFileReaderConfig;

/**
 * Tests {@link AsyncMultiFileReaderPipe}
 *
 * @author Eyal Schneider
 */
public class AsyncMultiFileReaderPipeTest {
  @Test
  public void testNoData() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      new File(tmpFolder, "data/latest").mkdirs();
      
      try (AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
          LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
          .paths(tmpFolder.getAbsolutePath() + "/data") // Intermediate empty folder
          .build());
          AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p);
          ) {
        w.start();
        assertTrue(w.getItems().isEmpty());
      }
    } finally {
      FileUtils.deleteFiles(tmpFolder);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 10})
  public void testSharding(int shardCount) throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data/latest");
      File dataFolder2 = new File(tmpStorageFolder, "data/latest2");
      dataFolder.mkdirs();
      dataFolder2.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(dataFolder, "b.csv"));
      FileUtils.writeLines(Arrays.asList("4", "3", "6"), new File(dataFolder, "c.csv"));
      FileUtils.writeLines(Arrays.asList("9", "10"), new File(dataFolder, "d.csv"));
      FileUtils.writeLines(Arrays.asList("7", "8"), new File(dataFolder2, "d.csv")); // Duplicate file name to check file name deduper
  
      List<String> readItems = new ArrayList<>();
      for (int shard = 0; shard < shardCount; shard++) {
        try (
            AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
                LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
                .paths(dataFolder, dataFolder2)
                .shard(new ShardSpecifier(shard, shardCount))
                .build());
            AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p)) {
          w.start();
          readItems.addAll(w.getItems());
        }
      }
      assertEquals(10, readItems.size());
      assertEquals(Sets.newHashSet("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), new HashSet<>(readItems));
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testFileFilter() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data/latest");
      File dataFolder2 = new File(tmpStorageFolder, "data/latest2");
      dataFolder.mkdirs();
      dataFolder2.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(dataFolder, "b.csv"));
      FileUtils.writeLines(Arrays.asList("4", "3", "6"), new File(dataFolder, "c.csv"));
      FileUtils.writeLines(Arrays.asList("9", "10"), new File(dataFolder, "d.csv"));
      FileUtils.writeLines(Arrays.asList("7", "8"), new File(dataFolder2, "d.csv")); // Duplicate file name to check file name deduper
  
      try (
          AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder, dataFolder2)
              .andFilter(f -> f.getAbsolutePath().endsWith("d.csv"))
              .build());
          AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Sets.newHashSet("9", "10", "7", "8"), new HashSet<>(w.getItems()));
      }
      
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testOriginFileMetadata() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data");
      dataFolder.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(dataFolder, "b.csv"));
      FileUtils.writeLines(Arrays.asList("4", "3", "6"), new File(dataFolder, "c.csv"));
      FileUtils.writeLines(Arrays.asList("9", "10"), new File(dataFolder, "d.csv"));
  
      try (
          AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new FileNamePrefixPipeSupplier())
              .paths(dataFolder)
              .build());
          AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Sets.newHashSet("a_1", "a_5", "b_2", "c_4", "c_3", "c_6", "d_9", "d_10"), new HashSet<>(w.getItems()));
      }
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testUsingDecoderAsFileHandler() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data");
      dataFolder.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(dataFolder, "b.csv"));
      FileUtils.writeLines(Arrays.asList("4", "3", "6"), new File(dataFolder, "c.csv"));
      FileUtils.writeLines(Arrays.asList("9", "10"), new File(dataFolder, "d.csv"));
  
      try (
          AsyncPipe<Integer> p = new AsyncMultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TxtDecoderFactory<>(Integer::parseInt))
              .paths(dataFolder)
              .build());
          AsyncCollectionWriterPipe<Integer> w = new AsyncCollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Sets.newHashSet(1, 5, 2, 4, 3, 6, 9, 10), new HashSet<>(w.getItems()));
      }
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testRecursive() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data");
      File nestedFolder = new File(dataFolder, "nested");
      nestedFolder.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(nestedFolder, "b.csv"));
  
      // Non recursive (default)
      try (
          AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder.getAbsolutePath())
              .build());
          AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Sets.newHashSet("1", "5"), new HashSet<>(w.getItems()));
      }
  
      // Recursive
      try (
          AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder.getAbsolutePath(), true)
              .build());
          AsyncCollectionWriterPipe<String> w = new AsyncCollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Sets.newHashSet("1", "5", "2"), new HashSet<>(w.getItems()));
      }
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testBalancedSharding() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    
    try {
      File dataFolder = new File(tmpStorageFolder, "data");
      dataFolder.mkdirs();
      
      FileUtils.writeLines(Arrays.asList("0"), new File(dataFolder, "file1.csv"));
      FileUtils.writeLines(Arrays.asList("1", "2"), new File(dataFolder, "file2.csv"));
      FileUtils.writeLines(Arrays.asList("3", "4", "5", "6"), new File(dataFolder, "file3.csv"));
      FileUtils.writeLines(Arrays.asList("7", "8", "9", "10", "11", "12", "13", "14"), new File(dataFolder, "file4.csv"));
      
      int[] itemCounterPerWorker = new int[2];
      List<String> itemsRead = Collections.synchronizedList(new ArrayList<>());
      for (int w = 0; w < 2; w++) {
        int workerIndex = w;
        try (
            AsyncPipe<String> p = new AsyncMultiFileReaderPipe<>(
                LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
                .paths(dataFolder.getAbsolutePath(), true)
                .shard(new ShardSpecifier(w, 2), true)
                .build());
            TerminalPipe t = new AsyncConsumerPipe<>(p, v -> { itemsRead.add(v); itemCounterPerWorker[workerIndex]++;});
            ) {
          t.start();
        }
      }
      assertEquals(15, itemsRead.size());
      int worker1ItemCount = itemCounterPerWorker[0];
      int worker2ItemCount = itemCounterPerWorker[1];
      assertEquals(8, Math.max(worker1ItemCount, worker2ItemCount));
      assertEquals(7, Math.min(worker1ItemCount, worker2ItemCount));
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  private static class TestPipeSupplier implements PipeReaderSupplier<String, File> {
    @Override
    public Pipe<String> get(SizedInputStream is, File metadata)
        throws IOException, PipeException {
      return new TxtFileReaderPipe(is, StandardCharsets.UTF_8, 8196, Compression.NONE);
    }
  }
  
  private static class FileNamePrefixPipeSupplier implements PipeReaderSupplier<String, File> {
    @Override
    public Pipe<String> get(SizedInputStream is, File metadata)
        throws IOException, PipeException {
      String filename = FileUtils.removeExtension(metadata.getName());
      Pipe<String> txtP = new TxtFileReaderPipe(is, StandardCharsets.UTF_8, 8196, Compression.NONE);
      return new MapPipe<>(txtP, line -> filename + "_" + line);
    }
  }

}
