package org.pipecraft.pipes.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.pipecraft.infra.io.FileReadOptions;

/**
 * Allows creating {@link ItemDecoder} instances of some specific type.
 * 
 * This is the preferred type to be given to pipes. This allows the pipe to create dynamically as many instances
 * as needed, thus solving threading issues and state issues easily.
 * 
 * This interface includes a getByteArrayDecoder(..) method with default implementation.
 * This decoder serves for decoding individual items which are given as byte arrays rather than being
 * part of an input stream. The default implementation is not very efficient, and sub-classes are encouraged
 * to override it.
 * 
 * @param <T> The decoded items' data type
 *
 * @author Eyal Schneider
 */
public interface DecoderFactory <T> {
  /**
   * @param is The input stream the decoder should be bound to. Not expected to be buffered.
   * Buffering is added by the decoder.
   * @param readOptions defines how the input stream should be handled
   * @return The new decoder
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException;

  /**
   * @param is The input stream the decoder should be bound to
   * @return The new decoder
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  default ItemDecoder<T> newDecoder(InputStream is) throws IOException {
    return newDecoder(is, new FileReadOptions());
  }

  /**
   * @return A new {@link ByteArrayDecoder}, able to efficiently decode individual
   * items from byte arrays. The default implementation goes through a {@link ByteArrayInputStream} and is not very efficient, 
   * so sub-classes are encouraged to override it when a faster alternative is available.
   */
  default ByteArrayDecoder<T> newByteArrayDecoder() {
    return bytes -> {
      try (
          ItemDecoder<T> decoder = newDecoder(new ByteArrayInputStream(bytes))) {
        return decoder.decode();
      } catch (IOException e) {
        throw new RuntimeException(e); //Unreachable
      }
    };
  }
}
