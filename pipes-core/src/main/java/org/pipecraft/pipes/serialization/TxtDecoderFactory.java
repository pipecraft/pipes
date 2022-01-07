package org.pipecraft.pipes.serialization;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * An {@link DecoderFactory} that decodes items from a textual form, assuming that items are delimited by "\n".
 * See also {@link TxtEncoderFactory}.
 * 
 * @param <T> The data type of the decoded items
 * 
 * @author Eyal Schneider
 */
public class TxtDecoderFactory<T> implements DecoderFactory<T> {
  // A simple decoder for string items, decoding the string itself as is, using UTF8
  public static final TxtDecoderFactory<String> IDENTITY = new TxtDecoderFactory<>(x -> x);
  private final FailableFunction<String, T, ? extends ValidationPipeException> itemDetextualizer;
  private final Charset charset;

  /**
   * Constructor
   * 
   * @param itemDetextualizer A converter from the textual form to the item itself
   * @param charset The charset encoding to use for decoding
   */
  public TxtDecoderFactory(FailableFunction<String, T, ? extends ValidationPipeException> itemDetextualizer, Charset charset) {
    this.itemDetextualizer = itemDetextualizer;
    this.charset = charset;
  }

  /**
   * Constructor
   *
   * Assumes UTF8 text encoding.
   * 
   * @param itemDetextualizer A converter from the textual form to the item itself
   */
  public TxtDecoderFactory(FailableFunction<String, T, ? extends ValidationPipeException> itemDetextualizer) {
    this(itemDetextualizer, StandardCharsets.UTF_8);
  }
  
  @Override
  public ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    return new Decoder(is, readOptions);
  }

  @Override
  public ByteArrayDecoder<T> newByteArrayDecoder() {
    return bytes -> {
      String itemAsText = new String(bytes, charset);
      return itemDetextualizer.apply(itemAsText);
    };
  }

  private class Decoder implements ItemDecoder<T> {
    private final BufferedReader reader;
    
    /**
     * Constructor
     * 
     * @param is The input stream bound to this decoder. Not expected to be buffered.
     * @param options The read options to apply on the input stream
     * @throws IOException In case of an IO error while preparing to read from the input stream
     */
    public Decoder(InputStream is, FileReadOptions options) throws IOException {
      this.reader = FileUtils.getReader(is, charset, options);
    }

    @Override
    public T decode() throws IOException, ValidationPipeException {
      String itemAsText = reader.readLine();
      if (itemAsText == null) {
        return null;
      }
      return itemDetextualizer.apply(itemAsText);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
