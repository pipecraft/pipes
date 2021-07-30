package org.pipecraft.infra.math;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A fast version of {@link Random}, which extends from it but eliminates thread safety.
 * 
 * {@link ThreadLocalRandom} has a similar performance, but by design it
 * doesn't allow specifying the seed. Usage of 3rd patry implementations is also possible, but this
 * implementation is guaranteed to have the same behavior of java.util.Random, given the same seed.
 * 
 * @author Eyal Schneider
 *
 */
public class FastRandom extends Random {
  private static final long serialVersionUID = 1L;

  private static final AtomicLong seedUniquifier = new AtomicLong(8682522807148012L);

  private static final long multiplier = 0x5DEECE66DL;
  private static final long addend = 0xBL;
  private static final long mask = (1L << 48) - 1;

  private long seed;

  /**
   * Constructor
   * 
   * Uses a seed derived from the clock
   */
  public FastRandom() {
    this(seedUniquifier() ^ System.nanoTime());
  }

  private static long seedUniquifier() {
    // L'Ecuyer, "Tables of Linear Congruential Generators of
    // Different Sizes and Good Lattice Structure", 1999
    for (;;) {
      long current = seedUniquifier.get();
      long next = current * 181783497276652981L;
      if (seedUniquifier.compareAndSet(current, next))
        return next;
    }
  }

  /**
   * Constructor
   * 
   * @param seed The seed value to use
   */
  public FastRandom(long seed) {
    this.seed = (seed ^ multiplier) & mask;
  }

  @Override
  protected int next(int bits) {
    long oldseed, nextseed;
    oldseed = seed;
    nextseed = (oldseed * multiplier + addend) & mask;
    seed = nextseed;
    return (int) (nextseed >>> (48 - bits));
  }
}
