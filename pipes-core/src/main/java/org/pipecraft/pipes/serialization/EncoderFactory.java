package org.pipecraft.pipes.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.pipecraft.infra.io.FileWriteOptions;

/**
 * Allows creating {@link ItemEncoder} instances of some specific type.
 * 
 * This is the preferred type to be given to pipes. This allows the pipe to create dynamically as many instances
 * as needed, thus solving threading issues and state issues easily.
 * 
 * This interface includes a getByteArrayEncoder(..) method with default implementation.
 * This encoder serves for encoding individual items into separate byte arrays, rather than into
 * an output stream. The default implementation is not very efficient, and sub-classes are encouraged
 * to override it.
 * 
 * @param <T> The encoded items' data type
 *
 * @author Eyal Schneider
 */
public interface EncoderFactory <T> {
  /**
   * @param os The output stream the encoder should be bound to. Not expected to be buffered. 
   * The encoder handles buffering.
   * @param writeOptions The way the output stream should be handled
   * @return The new encoder
   * @throws IOException In case of an IO error while preparing to write to the output stream
   */
  ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException;

  /**
   * @param os The output stream the encoder should be bound to. Not expected to be buffered.
   * The encoder handles buffering.
   * @return The new encoder
   * @throws IOException In case of an IO error while preparing to write to the output stream
   */
  default ItemEncoder<T> newEncoder(OutputStream os) throws IOException {
    return newEncoder(os, new FileWriteOptions());
  }

  /**
   * @return A new {@link ByteArrayEncoder}, able to efficiently encode individual
   * items as byte arrays. The default implementation goes through a {@link ByteArrayOutputStream} and is not very efficient, 
   * so sub-classes are encouraged to override it when a faster alternative is available.
   */
  default ByteArrayEncoder<T> newByteArrayEncoder() {
    return item -> {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try (ItemEncoder<T> encoder = newEncoder(os)) {
        encoder.encode(item);
      } catch (IOException e) {
        throw new RuntimeException(e); // Unreachable
      }
      return os.toByteArray();
    };
  }
}
