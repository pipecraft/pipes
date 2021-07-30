package org.pipecraft.pipes.serialization;

import java.io.Closeable;
import java.io.IOException;

import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * A functional interface for stateful decoders, decoding object from some binary source.
 * Each decoder should have a corresponding {@link DecoderFactory}, which is the type that pipes work with.
 * 
 * Implementations typically receive a non-buffered input stream in their constructor, and do the
 * necessary pre-processing (such as adding a buffered wrapper) there.
 * 
 * @author Eyal Schneider
 */
public interface ItemDecoder<T> extends Closeable {
  /**
   * Reads and decodes the next object from the input stream.
   * @return The decoded item from the input stream provided on construction, or null if end of stream was reached
   * @throws IOException In case of a read error
   * @throws ValidationPipeException In case that the data for decoding is illegal
   */
  T decode() throws IOException, ValidationPipeException;
  
  // Default empty implementation, for making this interface a functional interface. 
  // Subclasses should override when relevant.
  @Override
  default void close() throws IOException {
  }

}
