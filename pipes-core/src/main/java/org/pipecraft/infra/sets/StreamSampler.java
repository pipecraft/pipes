package org.pipecraft.infra.sets;

import java.util.Random;

/**
 * A random sampler on a stream of a known size.
 * 
 * @author Eyal Schneider
 */
public class StreamSampler {
  private final int size;
  private int toSample;
  private int visited;
  private final Random rnd;

  /**
   * Constructor
   * 
   * @param streamSize The total size of the stream.
   * @param toSample The number of items to be sampled
   */
  public StreamSampler(Random rnd, int streamSize, int toSample) {
    this.size = streamSize;
    this.toSample = toSample;
    this.rnd = rnd;
  }
  
  /**
   * To be called per visited item.
   * @return
   */
  public boolean accept() {
    boolean accepted = false;
    if (rnd.nextDouble() < ((double)toSample) / (size - visited)){
      toSample--;
      accepted = true;
    }
    visited++;
    return accepted;
  }
}
