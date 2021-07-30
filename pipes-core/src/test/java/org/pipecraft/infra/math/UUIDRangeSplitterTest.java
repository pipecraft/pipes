package org.pipecraft.infra.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link UUIDRangeSplitter}
 *
 * @author Eyal Schneider
 */
public class UUIDRangeSplitterTest {
  @Test
  public void testSingleShard() {
    UUIDRangeSplitter sp = new UUIDRangeSplitter(1);
    assertEquals(0, sp.getShardFor("00000000-0000-0000-0000-000000000000"));
    assertEquals(0, sp.getShardFor("A0000000-0000-0000-0000-000000000000"));
    assertEquals(0, sp.getShardFor("ffffffff-ffff-ffff-ffff-ffffffffffff"));
  }

  @Test
  public void testSimpleSharding() {
    UUIDRangeSplitter sp = new UUIDRangeSplitter(22);
    for (char c = '0'; c <= '9'; c++) {
      assertEquals(c - '0', sp.getShardFor(c + "0000000-a000-0000-0000-000000000000"));
    }
    for (char c = 'A'; c <= 'F'; c++) {
      assertEquals((c - 'A') + 10, sp.getShardFor(c + "0000000-a000-0000-0000-000000000000"));
    }
    for (char c = 'a'; c <= 'f'; c++) {
      assertEquals((c - 'a') + 16, sp.getShardFor(c + "0000000-a000-0000-0000-000000000000"));
    }
  }

  @Test
  public void testMonotonicity() {
    Random rnd = new Random(180776);
    List<String> l = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      l.add(randomUUID(rnd));
    }
    l.sort(String::compareTo);

    for (int shardsCount : new int[]{1, 4, 16, 64, 256, 1024}) {
      UUIDRangeSplitter sp = new UUIDRangeSplitter(shardsCount);

      int prev = -1;
      for (String uuid : l) {
        int shard = sp.getShardFor(uuid);
        assertTrue(prev <= shard);
        prev = shard;
      }
    }
  }

  public static String randomUUID(Random rnd) {
    byte[] buffer = new byte[16];
    rnd.nextBytes(buffer);
    char[] chars0 = Hex.encodeHex(buffer, rnd.nextBoolean());
    char[] chars = new char[36];
    chars[8] = '-';
    chars[13] = '-';
    chars[18] = '-';
    chars[23] = '-';
    System.arraycopy(chars0, 0, chars, 0, 8);
    System.arraycopy(chars0, 8, chars, 9, 4);
    System.arraycopy(chars0, 12, chars, 14, 4);
    System.arraycopy(chars0, 16, chars, 19, 4);
    System.arraycopy(chars0, 20, chars, 24, 12);
    return new String(chars);
  }

}
