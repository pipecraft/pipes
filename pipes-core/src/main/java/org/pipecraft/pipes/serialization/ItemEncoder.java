package org.pipecraft.pipes.serialization;

import java.io.Closeable;
import java.io.IOException;

/**
 * A functional interface for stateful encoders, encoding object into some binary destination.
 * Each encoder should have a corresponding {@link EncoderFactory}, which is the type that pipes work with.
 * 
 * Implementations typically receive a non-buffered output stream in their constructor, and do the
 * necessary pre-processing (such as adding a buffered wrapper) there.
 * 
 * @author Eyal Schneider
 */
public interface ItemEncoder<T> extends Closeable {
  /**
   * @param item The item to encode into the output stream provided at construction type
   * @throws IOException In case of write error
   */
  void encode(T item) throws IOException;
}
