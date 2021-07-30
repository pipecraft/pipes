package org.pipecraft.pipes.serialization;

/**
 * An interface for byte array encoder and decoder
 * 
 * @param <T> the type of the items to encode/decode
 *
 * @author Eyal Schneider
 */
public interface ByteArrayCodec<T> extends ByteArrayEncoder<T>, ByteArrayDecoder<T> {
}
