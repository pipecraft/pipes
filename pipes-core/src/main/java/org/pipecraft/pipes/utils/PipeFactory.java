package org.pipecraft.pipes.utils;

import java.io.IOException;
import java.util.function.Function;

import org.pipecraft.pipes.sync.Pipe;

/**
 * An interface for pipe creation based on some parameter. The standard {@link Function} interface isn't good enough, because we want to allow IOExceptions.
 * 
 * @param <P> The type of the parameter used for pipe creation
 * @param <T> The type of the items the produced pipes work with
 * 
 * @author Eyal Schneider
 */
public interface PipeFactory <P, T> {
  /**
   * @param param A parameter for the pipe creation
   * @return The created pipe
   * @throws IOException In case of creation error due to IO problem
   */
  Pipe<T> get(P param) throws IOException;
}
