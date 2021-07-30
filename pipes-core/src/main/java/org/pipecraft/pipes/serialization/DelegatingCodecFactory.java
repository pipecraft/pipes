package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileWriteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Wraps a given encoder and decoder factories, and delegates encoding/decoding creation
 * requests to them.
 * 
 * @param <T> The encoded/decoded items' data type
 * 
 * @author Eyal Schneider
 */
public class DelegatingCodecFactory <T> implements CodecFactory<T> {
  private final EncoderFactory<T> encoder;
  private final DecoderFactory<T> decoder;

  /**
   * Constructor
   * @param encoder The encoder factory
   * @param decoder The decoder factory
   */
  public DelegatingCodecFactory(EncoderFactory<T> encoder, DecoderFactory<T> decoder) {
    this.encoder = encoder;
    this.decoder = decoder;
  }

  @Override
  public ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    return decoder.newDecoder(is,readOptions);
  }

  @Override
  public ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    return encoder.newEncoder(os, writeOptions);
  }
}
