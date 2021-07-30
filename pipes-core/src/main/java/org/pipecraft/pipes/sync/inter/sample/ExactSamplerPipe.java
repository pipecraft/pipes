package org.pipecraft.pipes.sync.inter.sample;

import java.util.Random;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.inter.FilterBasePipe;

/**
 * A pipe sampling exactly m items of an input pipe containing n items, where n is known in advance.
 * The algorithm is an online algorithm choosing a given item with probability {#remaining items to sample}/{#remaining items to visit}.
 * 
 * @author Eyal Schneider
 */
public class ExactSamplerPipe<T> extends FilterBasePipe<T> {
  private final Random rnd;
  private long remaining;
  private long remainingToSample;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param n The exact number of items in the input pipe. If inaccurate, sampling won't be complete or won't be uniform.
   * @param m The number of items to sample
   * @param rnd The randomizer to use, if consistency is required
   */
  public ExactSamplerPipe(Pipe<T> input, int n, int m, Random rnd) {
    super(input);
    this.remaining = n;
    this.remainingToSample = m;
    this.rnd = rnd;
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param n The exact number of items in the input pipe. If inaccurate, sampling won't be complete or won't be uniform.
   * @param m The number of items to sample
   */
  public ExactSamplerPipe(Pipe<T> input, int n, int m) {
    this(input, n, m, new Random());
  }

  @Override
  protected boolean shouldSelect(T item) {
    if (rnd.nextDouble() < remainingToSample / (double)remaining) {
      remaining--;
      remainingToSample--;
      return true;
    }
    remaining--;
    return false;
  }
}
