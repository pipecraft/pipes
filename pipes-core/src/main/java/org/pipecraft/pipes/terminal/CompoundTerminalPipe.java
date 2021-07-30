package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;

/**
 * A terminal pipe encapsulating a pipeline.
 * Subclasses should implement createPipeline() method.
 * 
 * @author Eyal Schneider
 *
 */
public abstract class CompoundTerminalPipe extends TerminalPipe {
  private TerminalPipe innerPipe;
  
  @Override
  public void close() throws IOException {
    if (innerPipe != null) {
      innerPipe.close();
    }
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    innerPipe = createPipeline();
    innerPipe.start();
  }

  /**
   * @return A new terminal pipeline to represent the logic of this pipe
   * @throws PipeException In case of a pipeline creation error
   * @throws InterruptedException In case that the thread is interrupted
   */
  protected abstract TerminalPipe createPipeline() throws PipeException, InterruptedException;

  @Override
  public float getProgress() {
    return innerPipe.getProgress();
  }
}
