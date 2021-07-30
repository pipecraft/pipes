package org.pipecraft.pipes.utils;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.exceptions.PipeException;

import java.io.IOException;

/**
 * An interface for reader pipe creators.
 *
 * @param <T> The type of the items the produced pipes work with
 * @param <M> The metadata type
 * 
 * @author Zacharya Haitin
 */
public interface PipeReaderSupplier<T, M> {
  /**
   * @param is SizedInputStream the pipe should read from.
   * @param metadata some additional data describing the data source
   * @return The created pipe.
   * @throws IOException In case of creation error due to IO problem.
   */
  Pipe<T> get(SizedInputStream is, M metadata) throws IOException, PipeException;
}
