package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Functions;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;
import org.pipecraft.pipes.sync.source.SeqGenPipe;

/**
 * Tests {@link SharderByHashPipe}
 * 
 * @author Eyal Schneider
 *
 */
public class SharderByHashPipeTest {
  @Test
  public void test() throws Exception {
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
