package org.pipecraft.pipes.sync.inter;

import java.io.IOException;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A pipe counting the items in the input pipe, and returning the count as a single output item.
 * 
 * @author Eyal Schneider
 */
public class CountPipe implements Pipe<Integer> {
  private final Pipe<?> input;
  private Integer count;
  
  /**
   * constructor
   * 
   * @param input The input pipe
   */
  public CountPipe(Pipe<?> input) {
    this.input = input;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    int count = 0;
    while (input.next() != null) {
      count++;
    }
    this.count = count;
  }
  
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public Integer next() throws PipeException, InterruptedException {
    Integer res = count;
    count = null;
    return res;
  }

  @Override
  public Integer peek() {
    return count;
  }

  @Override
  public float getProgress() {
    return 1.0f; // The work is done in the start() method
  }
}
