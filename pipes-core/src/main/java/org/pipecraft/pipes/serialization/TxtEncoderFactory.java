package org.pipecraft.pipes.serialization;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;

/**
 * An {@link EncoderFactory} that encodes items as text, delimiting them by "\n".
 * See also {@link TxtDecoderFactory}.
 * 
 * @param <T> The data type of the items to encode
 * 
 * @author Eyal Schneider
 */
public class TxtEncoderFactory<T> implements EncoderFactory<T> {
  private final Function<T, String> itemTextualizer;
  private final Charset charset;

  /**
   * Constructor
   * 
   * @param itemTextualizer A converter from the object to its textual form
   * @param charset The charset encoding to use for encoding
   */
  public TxtEncoderFactory(Function<T, String> itemTextualizer, Charset charset) {
    this.itemTextualizer = itemTextualizer;
    this.charset = charset;
  }

  /**
   * Constructor
   *
   * Assumes UTF8 text encoding
   * 
   * @param itemTextualizer A converter from the object to its textual form
   */
  public TxtEncoderFactory(Function<T, String> itemTextualizer) {
    this(itemTextualizer, StandardCharsets.UTF_8);
  }

  /**
   * Constructor
   *
   * Assumes UTF8 text encoding and textualization based on the object's toString() 
   */
  public TxtEncoderFactory() {
    this(Object::toString, StandardCharsets.UTF_8);
  }

  @Override
  public ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    return new Encoder(os, writeOptions);
  }

  @Override
  public ByteArrayEncoder<T> newByteArrayEncoder() {
    return item -> {
      String itemAsText = itemTextualizer.apply(item);
      return itemAsText.getBytes(charset);
    };
  }

  private class Encoder implements ItemEncoder<T> {
    private final BufferedWriter writer;
    
    /**
     * Constructor
     * 
     * @param os The output stream bound to this encoder. Not expected to be buffered.
     * @param options The read options to apply on the output stream
     * @throws IOException in case of an IO error while preparing to write to the output stream
     */
    public Encoder(OutputStream os, FileWriteOptions options) throws IOException {
      this.writer = FileUtils.getWriter(os, charset, options);
    }

    @Override
    public void encode(T item) throws IOException {
      String itemAsText = itemTextualizer.apply(item);
      writer.write(itemAsText);
      writer.newLine();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }
}
