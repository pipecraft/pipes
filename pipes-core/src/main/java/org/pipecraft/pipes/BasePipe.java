package org.pipecraft.pipes;

import java.io.Closeable;

import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A base interface containing minimal API for sync, async and terminal pipes
 * 
 * @author Eyal Schneider
 */
public interface BasePipe extends Closeable {
  /**
   * Performs pre-processing prior to item flow throw the pipe. 
   * Implementations must call the same method for all their input pipes before accessing their items. This is typically done here.
   * @throws PipeException In case of pipe errors in this pipe or somewhere up-stream.
   * @throws InterruptedException In case that the operation has been interrupted by another thread.
   */
  void start() throws PipeException, InterruptedException;
  
  /**
   * @return The pipe flow progress, as a floating number between 0.0 and 1.0.
   * Important implementation rules:
   * 1) Calling this method before start() call is complete isn't allowed and has an undefined behavior.
   * 2) Implementation should do best effort to provide an estimate of the progress this pipe has made (0.0 - 1.0) 
   * 3) When the pipe is fully consumed, getProgress() should return 1.0.
   * 4) Results must be monotonous, i.e. results of consecutive calls may never be decreasing. 
   * 5) Thread safety: progress may be maintained by some thread/s but monitoring by other threads. Implementations must be thread safe.
   */
  float getProgress();
}
