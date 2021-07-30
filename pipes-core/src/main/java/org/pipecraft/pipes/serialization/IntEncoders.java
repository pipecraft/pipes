package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.Coding;

/**
 * A collection of encoder factories for writing 16, 32 and 64 bit integers
 * 
 * @author Eyal Schneider
 */
public class IntEncoders {
  public static final EncoderFactory<Integer> INT16_BE_ENCODER_FACTORY = new SimpleEncoderFactory<>(
      Coding::writeBigEndian16, Coding::getBigEndian16AsBytes);
  public static final EncoderFactory<Integer> INT16_LE_ENCODER_FACTORY = new SimpleEncoderFactory<>(Coding::writeLittleEndian16, Coding::getLittleEndian16AsBytes);
  public static final EncoderFactory<Integer> INT32_BE_ENCODER_FACTORY = new SimpleEncoderFactory<>(Coding::writeBigEndian32, Coding::getBigEndian32AsBytes);
  public static final EncoderFactory<Integer> INT32_LE_ENCODER_FACTORY = new SimpleEncoderFactory<>(Coding::writeLittleEndian32, Coding::getLittleEndian32AsBytes);
  public static final EncoderFactory<Long> INT64_BE_ENCODER_FACTORY = new SimpleEncoderFactory<>(Coding::writeBigEndian64, Coding::getBigEndian64AsBytes);
  public static final EncoderFactory<Long> INT64_LE_ENCODER_FACTORY = new SimpleEncoderFactory<>(Coding::writeLittleEndian64, Coding::getLittleEndian64AsBytes);
}
