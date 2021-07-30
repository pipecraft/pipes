package org.pipecraft.pipes.serialization;

import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.infra.io.Coding;
import org.pipecraft.infra.io.FileReadOptions;
import java.io.IOException;
import java.io.InputStream;

/**
 * A functional interface for decoders working on byte arrays
 * representing single items.
 * 
 * In contract to {@link ItemDecoder}, this decoder should always be stateless,
 * and is suitable for cases where the data to decode is already in an array,
 * whose bytes encode a single item to decode.
 * 
 * @param <T> The decoded item data type
 * 
 * @author Eyal Schneider
 */
public interface ByteArrayDecoder<T> {
  /**
   * @param bytes The byte representation of an item to decode
   * @return The decoded item
   * @throws ValidationPipeException In case that the data for decoding is illegal
   */
  T decode(byte[] bytes) throws ValidationPipeException;

  /**
   * Returns an efficient {@link ItemDecoder} based on this byte array decoder, assuming a fixed record size.
   * @param is The input stream to build the decoder for
   * @param readOptions The read options defining how the input stream should be manipulated
   * @param recSize The fixed record size
   * @return A new item decoder based on the given input stream, and using this byte array decoder under the hood
   * @throws IOException In case of IO error while preparing to read from the input stream
   */
  default ItemDecoder<T> newFixedRecSizeDecoder(InputStream is, FileReadOptions readOptions, int recSize) throws IOException {
    return new AbstractInputStreamItemDecoder<>(is, readOptions) {
      private final byte[] buffer = new byte[recSize];

      @Override
      public T decode() throws IOException, ValidationPipeException {
        if (!Coding.tryRead(is, buffer, 0, recSize)) {
          return null;
        }

        return ByteArrayDecoder.this.decode(buffer);
      }
    };
  }
}
