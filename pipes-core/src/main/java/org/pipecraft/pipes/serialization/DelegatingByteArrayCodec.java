package org.pipecraft.pipes.serialization;

import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * Wraps a given byte array encoder and byte array decoder, and delegates encoding/decoding 
 * requests to them.
 * 
 * @param <T> The encoded/decoded items' data type
 * 
 * @author Eyal Schneider
 */
public class DelegatingByteArrayCodec <T> implements ByteArrayCodec<T> {
  private final ByteArrayEncoder<T> encoder;
  private final ByteArrayDecoder<T> decoder;

  /**
   * Constructor
   * @param encoder The encoder
   * @param decoder The decoder
   */
  public DelegatingByteArrayCodec(ByteArrayEncoder<T> encoder, ByteArrayDecoder<T> decoder) {
    this.encoder = encoder;
    this.decoder = decoder;
  }

  @Override
  public byte[] encode(T item) {
    return encoder.encode(item);
  }

  @Override
  public T decode(byte[] bytes) throws ValidationPipeException {
    return decoder.decode(bytes);
  }
}
