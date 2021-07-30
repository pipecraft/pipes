package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.OutputStream;

import org.pipecraft.infra.concurrent.FailableBiConsumer;
import org.pipecraft.infra.io.FileWriteOptions;

/**
 * A simple generic encoder factory implementation.
 * Handles output stream buffering and closing. 
 * Uses a simplified stateless output stream encoder function given in the ctor.
 * 
 * @param <T> The data type of the objects to encode
 *
 * @author Eyal Schneider
 */
public class SimpleEncoderFactory <T> implements EncoderFactory<T> {
  private final FailableBiConsumer<T, OutputStream, ? extends IOException> statelessEncoder;
  private final ByteArrayEncoder<T> byteArrEncoder;

  /**
   * Constructor
   * 
   * @param statelessEncoder A consumer receiving a (buffered) output stream and a value, writing the value to the stream
   */
  public SimpleEncoderFactory(FailableBiConsumer<T, OutputStream, ? extends IOException> statelessEncoder) {
    this.statelessEncoder = statelessEncoder;
    this.byteArrEncoder = EncoderFactory.super.newByteArrayEncoder();
  }

  /**
   * Constructor
   * 
   * @param statelessEncoder A consumer receiving a (buffered) output stream and a value, writing the value to the stream
   * @param byteArrEncoder The byte array encoder to be returned by the newByteArrayEncoder() function.
   */
  public SimpleEncoderFactory(FailableBiConsumer<T, OutputStream, ? extends IOException> statelessEncoder, ByteArrayEncoder<T> byteArrEncoder) {
    this.statelessEncoder = statelessEncoder;
    this.byteArrEncoder = byteArrEncoder;
  }

  @Override
  public ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    return new Encoder(os, writeOptions);
  }

  @Override
  public ByteArrayEncoder<T> newByteArrayEncoder() {
    return byteArrEncoder;
  }

  private class Encoder extends AbstractOutputStreamItemEncoder<T> {
    public Encoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
      super(os, writeOptions);
    }

    @Override
    public void encode(T value) throws IOException {
      SimpleEncoderFactory.this.statelessEncoder.accept(value, os);
    }
  }
}
