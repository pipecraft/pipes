package org.pipecraft.pipes.serialization;

import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.infra.concurrent.FailableFunction;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * A textual encoder-decoder
 *
 * @param <T> The type of the item to encode/decode
 *
 * @author Eyal Schneider
 */
public class TxtCodecFactory <T> extends DelegatingCodecFactory <T> {

  /**
   * Constructor
   *
   * @param itemTextualizer A converter from the item to its textual form
   * @param itemDetextualizer A converter from the textual form to the item itself
   * @param charset The charset encoding to use for encoding
   */
  public TxtCodecFactory(Function<T, String> itemTextualizer, FailableFunction<String, T, ? extends ValidationPipeException> itemDetextualizer, Charset charset) {
    super(new TxtEncoderFactory<>(itemTextualizer, charset), new TxtDecoderFactory<>(itemDetextualizer, charset));
  }

  /**
   * Constructor
   *
   * Assumes UTF8 in encoded form
   * @param itemTextualizer A converter from the item to its textual form
   * @param itemDetextualizer A converter from the textual form to the item itself
   */
  public TxtCodecFactory(Function<T, String> itemTextualizer, FailableFunction<String, T, ? extends ValidationPipeException> itemDetextualizer) {
    super(new TxtEncoderFactory<>(itemTextualizer, StandardCharsets.UTF_8), new TxtDecoderFactory<>(itemDetextualizer, StandardCharsets.UTF_8));
  }

}
