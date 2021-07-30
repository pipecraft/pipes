package org.pipecraft.infra.math;

import java.util.List;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Arithmetics and math utilities
 * 
 * @author Eyal Schneider
 */
public final class ArithmeticUtils {
  private static final XXHash64 XXHASH = XXHashFactory.safeInstance().hash64();
  private static final long XXHASH_SEED = 1969; // Landing on the moon

  private static final int PRECISION = 1_000_000;
  
  /**
   * Private constructor.
   */
  private ArithmeticUtils() {}

  /**
   * <p>Uses the inclusion-exclusion principle to calculate the probability of a union of independent events Ei, each having a probability Pi
   * (P(E1-n) = P1 +P2 + Pn -P1P2 -P2P3 -P1P3 ... +P1P2P3 +... -
   * The solution is adding all the odd combination of the probabilities and minus all the even combination of the probabilities
   * For more info check https://en.wikipedia.org/wiki/Inclusion%E2%80%93exclusion_principle</p>
   *
   * Note : the algorithm is exponential and not suitable for large lists
   * @param probabilities a list of probabilities if this is an empty list 0 will return
   * @return probability of the union of the given probabilities
   */
  public static double getUnionProbability(List<Double> probabilities) {
    final int size = probabilities.size();
    final int powerSetSize = 1 << size;
    double odd = 0;
    double even = 0;

    for (int i = 1; i < powerSetSize; i++) { // each bit in i represents a position in the input list
      double mul = 1;
      for (int j = 0; j < size; j++) {
        if ((i & (1 << j)) != 0) {
          // if the bit is set in the current subset, its corresponded entity in the list will be included
          mul *= probabilities.get(j);
        }
      }

      // multiplications of even entities have a negative sign in inc-exc formula
      // The sign is a function of the parity of the number of bits turned on
      if ((Integer.bitCount(i) & 1) == 1) {
        odd += mul;
      } else {
        even += mul;
      }
    }
    return odd - even;
  }
  
  
  /**
   * Get consistent boolean value (accepted/rejected) by a given probability.
   * The consistency is based on a hash value of the given object.
   * @param object The object to either accept or reject
   * @param probability The probability for the object to be accepted
   */
  public static boolean getConsistentBooleanByProbability(Object object, float probability) {
    long hash = object.hashCode() & 0xFFFFFFFFL;
    long score = hash % PRECISION;
    return score < probability * PRECISION;
  }
  
  /**
   * @param obj A non-null object
   * @param n The total number of shards
   * @return The (deterministic) shard the object belongs to, in the range 0 ... n - 1. 
   * Assuming a good hash function, and a large enough population of objects the shards should tend to even sizes. 
   */
  public static int getShardByHash(Object obj, int n) {
    long hash = obj.hashCode() & 0xFFFFFFFFL; // Converts to positive long. Avoids a bias with signed hashcode. Using "abs(obj.hashCode()) % n" would result in twice less hits on bucket 0.  
    return (int) (hash % n);
  }

  /**
   * Similar to getShardByHash, but uses 64 bit hash and a stronger hashing algorithm and to achieve better balancing of shards
   * @param objBytes The object to hash, represented as a byte array
   * @param n The total number of shards
   * @return The (deterministic) shard the object belongs to, in the range 0 ... n - 1. 
   */
  public static int getShardByStrongHash(byte[] objBytes, int n) {
    long hash = XXHASH.hash(objBytes, 0, objBytes.length, XXHASH_SEED);
    hash &= 0x7FFFFFFFFFFFFFFFL; // Converts to positive long (losing 1 bit of info). Avoids a bias with signed hashcode. Using "abs(obj.hashCode()) % n" would result in twice less hits on bucket 0.  
    return (int) (hash % n);
  }
}
