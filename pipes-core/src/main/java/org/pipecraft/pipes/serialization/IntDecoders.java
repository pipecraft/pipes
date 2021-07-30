package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.Coding;

/**
 * A collection of decoder factories for reading 16, 32 and 64 bit integers
 * 
 * @author Eyal Schneider
 */
public class IntDecoders {
  public static final DecoderFactory<Integer> INT16_BE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Integer>)bytes -> Coding
      .readBigEndian16(bytes, 0)).newFixedRecSizeDecoder(is, options, 2);
  public static final DecoderFactory<Integer> INT16_LE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Integer>)bytes -> Coding.readLittleEndian16(bytes, 0)).newFixedRecSizeDecoder(is, options, 2);
  public static final DecoderFactory<Integer> INT32_BE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Integer>)bytes -> Coding.readBigEndian32(bytes, 0)).newFixedRecSizeDecoder(is, options, 4);
  public static final DecoderFactory<Integer> INT32_LE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Integer>)bytes -> Coding.readLittleEndian32(bytes, 0)).newFixedRecSizeDecoder(is, options, 4);
  public static final DecoderFactory<Long> INT64_BE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Long>)bytes -> Coding.readBigEndian64(bytes, 0)).newFixedRecSizeDecoder(is, options, 8);
  public static final DecoderFactory<Long> INT64_LE_DECODER_FACTORY = (is, options) -> ((ByteArrayDecoder<Long>)bytes -> Coding.readLittleEndian64(bytes, 0)).newFixedRecSizeDecoder(is, options, 8);
}
