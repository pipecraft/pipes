package org.pipecraft.infra.math;

/**
 * Splits the UUID value range into K equal sized continuous shards, and returns the thresholds.
 * Also allows determining the shard index given a UUID.
 * The UUID space is assumed to have all letter case representations, but a single UUID is assumed to be consistent with regards to letter case.
 * We assume plain string ordering, as defined by Java's String comparator.
 * This class is useful for sharding.
 *
 * @author Eyal Schneider
 */
public class UUIDRangeSplitter {
  private static final double HEX_DIGIT_FACTOR = 1.0 / 16.0;
  private static final double EXT_HEX_DIGIT_FACTOR = 1.0 / 22.0; // Refers to the whole range (0..9A..Fa..f)

  private final double shardSize;

  /**
   * Constructor
   *
   * @param shardsCount The required number of shards. Must be positive.
   */
  public UUIDRangeSplitter(int shardsCount) {
    this.shardSize = 1.0 / (double)shardsCount;
  }

  /**
   * Finds the shard corresponding to a given UUID
   * @param uuid The UUID. Must have a valid format, otherwise results are unpredictable.
   * @return The shard index, as an integer between 0 and shardsCount-1.
   */
  public int getShardFor(String uuid) {
    double res = 0.0;
    double w = EXT_HEX_DIGIT_FACTOR;
    double wm = EXT_HEX_DIGIT_FACTOR;
    for (int i = 0; i < uuid.length(); i++) {
      char c = uuid.charAt(i);
      if (c != '-') {
        int d = c - '0';
        if (d <= 9) { // Digit
          res += d * w;
        } else { // 'a'..'f' or 'A'..'F'
          d = c - 'A';
          if (d <= 5) { // 'A'..'F'
            res += (10 + d) * w;
          } else { // 'a'..'f'
            res += (10 + (wm == HEX_DIGIT_FACTOR ? 0 : 6) + (c - 'a')) * w;
          }
          wm = HEX_DIGIT_FACTOR;
        }
        //Verify if we are done
        int ind = (int) (res / shardSize);
        if (ind == (int) ((res + w) / shardSize)) { // We use additional hex digits only if they can change the shard index
          return ind;
        }
        w *= wm;
      }
    }
    return (int) (1 / shardSize);
  }
}
