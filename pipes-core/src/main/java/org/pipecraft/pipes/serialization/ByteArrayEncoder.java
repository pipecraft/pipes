package org.pipecraft.pipes.serialization;

/**
 * A functional interface for encoders producing byte arrays
 * representing single items.
 * 
 * In contract to {@link ItemEncoder}, this encoder should always be stateless,
 * and is suitable for cases where the encoded form is required to be a byte array,
 * whose bytes represent a single item.
 * 
 * @param <T> The encoded item data type
 * 
 * @author Eyal Schneider
 */
public interface ByteArrayEncoder<T> {
  /**
   * @param item The item to encode
   * @return The encoded item, as byte array
   */
  byte[] encode(T item);
}
