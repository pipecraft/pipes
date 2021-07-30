package org.pipecraft.pipes.sync.inter.sample;

import java.util.Random;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.inter.FilterBasePipe;

/**
 * A pipe sampling about x% of the incoming items.
 * The exact number of sampled items depends on the input pipe length and on the provided randomizer.
 * 
 * @author Eyal Schneider
 */
public class PortionSamplerPipe<T> extends FilterBasePipe<T> {
  private final double p;
  private final Random rnd;
  
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param p The portion of input items to sample. Must be between 0.0 and 1.0.
   * @param rnd The randomizer to use, if consistency is required
   */
  public PortionSamplerPipe(Pipe<T> input, double p, Random rnd) {
    super(input);
    this.p = p;
    this.rnd = rnd;
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param p The portion of input items to sample. Must be between 0.0 and 1.0.
   */
  public PortionSamplerPipe(Pipe<T> input, double p) {
    this(input, p, new Random());
  }

  @Override
  protected boolean shouldSelect(T item) {
    return rnd.nextDouble() < p;
  }
}
