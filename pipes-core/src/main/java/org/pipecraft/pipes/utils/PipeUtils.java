package org.pipecraft.pipes.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.pipecraft.pipes.BasePipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of pipe utility functions
 * 
 * @author Eyal Schneider
 */
public class PipeUtils {
  private static final Logger logger = LoggerFactory.getLogger(PipeUtils.class);
  /**
   * Closes the given pipes, ignoring errors. Useful in cases where the number of created pipes is dynamic, therefore try-with-resources
   * isn't an option.
   * 
   * @param pipes The collection of pipes to close. May contain null values. 
   */
  public static void close(Collection<? extends BasePipe> pipes) {
    for (BasePipe pipe : pipes) {
      try {
        if (pipe != null) {
          pipe.close();
        }
      } catch (IOException e) {
        logger.warn("Unable to close pipe of type " + pipe.getClass().getSimpleName(), e);
      }
    }
  }
  
  /**
   * Closes the given pipes, ignoring errors. Useful in cases where the number of created pipes is dynamic, therefore try-with-resources
   * isn't an option.
   * 
   * @param pipes The pipes to close. May contain null values. 
   */
  public static void close(BasePipe ... pipes) {
    close(Arrays.asList(pipes));
  }
  
  /**
   * @param pipes A collection of pipes, all assumed to be started
   * @return The maximum progress among the pipes' progress values
   */
  public static float getMaxProgress(Collection<? extends BasePipe> pipes) {
    float max = 0;
    for (BasePipe p : pipes) {
      float progress = p.getProgress();
      if (progress > max) {
        max = progress;
      }
    }
    return max;
  }
  
  /**
   * @param pipes A collection of pipes, all assumed to be started
   * @return The minimum progress among the pipes' progress values
   */
  public static float getMinProgress(Collection<? extends BasePipe> pipes) {
    float min = 1.0f;
    for (BasePipe p : pipes) {
      float progress = p.getProgress();
      if (progress < min) {
        min = progress;
      }
    }
    return min;
  }

  /**
   * @param pipes A set of pipes
   * @return The average progress of the input pipes at the moment
   */
  public static float getAverageProgress(Collection<? extends BasePipe> pipes) {
    double sum = 0.0;
    for (BasePipe p : pipes) {
      sum += p.getProgress();
    }
    return (float) (sum / pipes.size());
  }

}
