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
 * Tests {@link AsyncSharderByHashPipe}
 * 
 * @author Eyal Schneider
 */
public class AsyncSharderByHashPipeTest {

  @Test
  public void testSuccess() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("Test");
    try (
        DummyAsyncPipe source = new DummyAsyncPipe(0, 9, true);
        AsyncSharderPipe<Integer> sharder = new AsyncSharderByHashPipe<>(source, new TxtEncoderFactory<>(v -> v.toString()), 2, tmpFolder);
        ) {
      sharder.start();
      assertEquals(2, tmpFolder.list().length);
      List<String> f0Lines = getFileLines("0", tmpFolder);
      List<String> f1Lines = getFileLines("1", tmpFolder);
      assertEquals(9, f0Lines.size() + f1Lines.size());
      HashSet<String> all = new HashSet<>(f0Lines);
      all.addAll(f1Lines);
      assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8"), all);
      
      assertEquals(2, sharder.getShardSizes().size());
      assertEquals(9, sharder.getShardSizes().get("0") + sharder.getShardSizes().get("1"));
    } finally {
      FileUtils.deleteFiles(tmpFolder);
    }
  }
  
  @Test
  public void testError() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("Test");
    try (
        DummyAsyncPipe source = new DummyAsyncPipe(0, 10, false); // Reports error after 10 items
        AsyncSharderPipe<Integer> sharder = new AsyncSharderByHashPipe<>(source, new TxtEncoderFactory<>(v -> v.toString()), 2, tmpFolder);
        ) {
      assertThrows(ValidationPipeException.class, () -> sharder.start());
    } finally {
      FileUtils.deleteFiles(tmpFolder);
    }
  }

  private static List<String> getFileLines(String shardId, File folder) throws IOException {
    return FileUtils.getLinesFromFile(new File(folder, shardId));
  }
}
