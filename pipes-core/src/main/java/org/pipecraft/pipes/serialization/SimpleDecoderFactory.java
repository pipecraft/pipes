package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.InputStream;

import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * A simple generic decoder factory implementation.
 * Handles input stream buffering and closing.
 * Uses a simplified stateless input stream decoder function given in the ctor.
 * 
 * @param <T> The data type of the decoded objects
 *
 * @author Eyal Schneider
 */
public class SimpleDecoderFactory <T> implements DecoderFactory<T> {
  private final FailableFunction<InputStream, T, ? extends IOException> statelessDecoder;
  private final ByteArrayDecoder<T> byteArrDecoder;

  /**
   * Constructor
   * 
   * @param statelessDecoder A function receiving a (buffered) input stream, reading and returning the next item read from it.
   */
  public SimpleDecoderFactory(FailableFunction<InputStream, T, ? extends IOException> statelessDecoder) {
    this.statelessDecoder = statelessDecoder;
    this.byteArrDecoder = DecoderFactory.super.newByteArrayDecoder();
  }

  /**
   * Constructor
   * 
   * @param statelessDecoder A function receiving a (buffered) input stream, reading and returning the next item read from it.
   * @param byteArrDecoder The byte array decoder to be returned by the newByteArrayDecoder() function.
   */
  public SimpleDecoderFactory(FailableFunction<InputStream, T, ? extends IOException> statelessDecoder, ByteArrayDecoder<T> byteArrDecoder) {
    this.statelessDecoder = statelessDecoder;
    this.byteArrDecoder = byteArrDecoder;
  }

  @Override
  public ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    return new Decoder(is, readOptions);
  }

  @Override
  public ByteArrayDecoder<T> newByteArrayDecoder() {
    return byteArrDecoder;
  }

  private class Decoder extends AbstractInputStreamItemDecoder<T> {
    public Decoder(InputStream is, FileReadOptions readOptions) throws IOException {
      super(is, readOptions);
    }

    @Override
    public T decode() throws IOException, ValidationPipeException {
      return SimpleDecoderFactory.this.statelessDecoder.apply(is);
    }
  }
}
