package org.pipecraft.pipes.utils;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * An interface for pipe creators. The standard {@link Supplier} interface isn't good enough, 
 * because we want to allow {@link IOException} as well as {@link PipeException}.
 *
 * @param <T> The type of the items the produced pipes work with
 * 
 * @author Eyal Schneider
 */
public interface PipeSupplier<T> {

  /**
   * @return The created pipe
   * @throws IOException In case of creation error due to IO problem
   * @throws PipeException In case of other pipeline related problems preventing the creation from completing
   * @throws InterruptedException In case that the current thread is interrupted while producing the pipe
   */
  Pipe<T> get() throws IOException, PipeException, InterruptedException;
}
