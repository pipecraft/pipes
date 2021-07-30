package org.pipecraft.pipes.sync.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.Sets;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.sync.inter.MapPipe;
import org.pipecraft.pipes.serialization.TxtDecoderFactory;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.ConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import org.pipecraft.pipes.utils.PipeReaderSupplier;
import org.pipecraft.pipes.utils.ShardSpecifier;
import org.pipecraft.pipes.utils.multi.LocalMultiFileReaderConfig;

/**
 * Tests {@link MultiFileReaderPipe}
 *
 * @author Eyal Schneider
 */
public class MultiFileReaderPipeTest {
  @Test
  public void testNoData() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      new File(tmpFolder, "data/latest").mkdirs();
      
      try (Pipe<String> p = new MultiFileReaderPipe<>(
          LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
          .paths(tmpFolder.getAbsolutePath() + "/data") // Intermediate empty folder
          .build())) {
        p.start();
        assertNull(p.next());
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
            Pipe<String> p = new MultiFileReaderPipe<>(
                LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
                .paths(dataFolder, dataFolder2)
                .shard(new ShardSpecifier(shard, shardCount))
                .build());
            CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
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
          Pipe<String> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder, dataFolder2)
              .andFilter(f -> f.getAbsolutePath().endsWith("d.csv"))
              .build());
          CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList("9", "10", "7", "8"), w.getItems());
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
          Pipe<String> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new FileNamePrefixPipeSupplier())
              .paths(dataFolder)
              .build());
          CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList("a_1", "a_5", "b_2", "c_4", "c_3", "c_6", "d_9", "d_10"), w.getItems());
      }
    } finally {
      FileUtils.deleteFiles(tmpStorageFolder);
    }
  }

  @Test
  public void testNonDefaultOrdering() throws Exception {
    File tmpStorageFolder = FileUtils.createTempFolder("multiReaderTest");
    try {
      File dataFolder = new File(tmpStorageFolder, "data");
      dataFolder.mkdirs();
      FileUtils.writeLines(Arrays.asList("1", "5"), new File(dataFolder, "a.csv"));
      FileUtils.writeLines(Arrays.asList("2"), new File(dataFolder, "b.csv"));
      FileUtils.writeLines(Arrays.asList("4", "3", "6"), new File(dataFolder, "c.csv"));
      FileUtils.writeLines(Arrays.asList("9", "10"), new File(dataFolder, "d.csv"));
  
      try (
          Pipe<String> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder)
              .fileOrder((f1, f2) -> f2.getName().compareTo(f1.getName())) // Reverse lexicographic file name order
              .build());
          CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList("9", "10", "4", "3", "6", "2", "1", "5"), w.getItems());
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
          Pipe<Integer> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TxtDecoderFactory<>(Integer::parseInt))
              .paths(dataFolder)
              .build());
          CollectionWriterPipe<Integer> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList(1, 5, 2, 4, 3, 6, 9, 10), w.getItems());
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
          Pipe<String> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder.getAbsolutePath())
              .build());
          CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList("1", "5"), w.getItems());
      }
  
      // Recursive
      try (
          Pipe<String> p = new MultiFileReaderPipe<>(
              LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
              .paths(dataFolder.getAbsolutePath(), true)
              .build());
          CollectionWriterPipe<String> w = new CollectionWriterPipe<>(p)) {
        w.start();
        assertEquals(Arrays.asList("1", "5", "2"), w.getItems());
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
      List<String> itemsRead = new ArrayList<>();
      for (int w = 0; w < 2; w++) {
        int workerIndex = w;
        try (
            Pipe<String> p = new MultiFileReaderPipe<>(
                LocalMultiFileReaderConfig.builder(new TestPipeSupplier())
                .paths(dataFolder.getAbsolutePath(), true)
                .shard(new ShardSpecifier(w, 2), true)
                .build());
            TerminalPipe t = new ConsumerPipe<>(p, v -> { itemsRead.add(v); itemCounterPerWorker[workerIndex]++;});
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
