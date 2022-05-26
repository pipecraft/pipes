package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import org.pipecraft.pipes.async.DummyAsyncPipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;

/**
 * Tests {@link AsyncSharderPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncSharderPipeTest {

  @Test
  public void testSuccess() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("Test");
    try (
        DummyAsyncPipe source = new DummyAsyncPipe(0, 9, true);
        AsyncSharderPipe<Integer> sharder = new AsyncSharderPipe<>(source, new TxtEncoderFactory<>(Object::toString), i -> String.valueOf(i % 2) + ".csv", tmpFolder);
        ) {
      sharder.start();
      assertEquals(2, tmpFolder.list().length);
      List<String> f0Lines = getFileLines("0.csv", tmpFolder);
      assertEquals(5, f0Lines.size());
      assertEquals(Sets.newHashSet("0", "2", "4", "6", "8"), new HashSet<>(f0Lines));
      List<String> f1Lines = getFileLines("1.csv", tmpFolder);
      assertEquals(4, f1Lines.size());
      assertEquals(Sets.newHashSet("1", "3", "5", "7"), new HashSet<>(f1Lines));
      
      assertEquals(2, sharder.getShardSizes().size());
      assertEquals(5, sharder.getShardSizes().get("0.csv").intValue());
      assertEquals(4, sharder.getShardSizes().get("1.csv").intValue());
    } finally {
      FileUtils.deleteFiles(tmpFolder);
    }
  }
  
  @Test
  public void testError() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("Test");
    try (
        DummyAsyncPipe source = new DummyAsyncPipe(0, 10, false); // Reports error after 10 items
        AsyncSharderPipe<Integer> sharder = new AsyncSharderPipe<>(source, new TxtEncoderFactory<>(Object::toString), i -> String.valueOf(i % 2), tmpFolder);
        ) {
      assertThrows(ValidationPipeException.class, sharder::start);
    } finally {
      FileUtils.deleteFiles(tmpFolder);
    }
  }

  private static List<String> getFileLines(String shardId, File folder) throws IOException {
    return FileUtils.getLinesFromFile(new File(folder, shardId));
  }
}
