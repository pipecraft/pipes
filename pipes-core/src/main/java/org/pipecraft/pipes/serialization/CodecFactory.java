package org.pipecraft.pipes.serialization;

/**
 * A factory of both encoders and decoders
 * 
 * @param <T> The encoded/decoded items' data type
 * 
 * @author Eyal Schneider
 */
public interface CodecFactory <T> extends EncoderFactory<T>, DecoderFactory<T> {
  /**
   * @return A byte array encoder/decoder derived from this codec
   */
  default ByteArrayCodec<T> getByteArrayCodec() {
    return new DelegatingByteArrayCodec<>(newByteArrayEncoder(), newByteArrayDecoder());
  }
}
