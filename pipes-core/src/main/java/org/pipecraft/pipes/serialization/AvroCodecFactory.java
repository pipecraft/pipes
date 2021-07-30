package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;

/**
 * A codec factory for AVRO format.
 * Bound to a data type provided at factory creation time.
 * 
 * @param <T> The data type we wish to encode/decode
 *
 * @author Eyal Schneider
 */
public class AvroCodecFactory <T> implements CodecFactory<T> {
  private final Class<T> clazz;

  /**
   * Constructor
   * 
   * @param clazz The java class representing the type to encode
   */
  public AvroCodecFactory(Class<T> clazz) {
    this.clazz = clazz;
  }

  // Note that only compression type from the write options is respected here, since buffering is managed internally.
  @Override
  public ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    return new AvroEncoder<>(clazz, FileUtils.getCompressionOutputStream(os, writeOptions.getCompression()));
  }

  // Note that only compression type from the read options is respected here, since buffering is managed internally.
  @Override
  public ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    return new AvroDecoder<>(clazz, FileUtils.getCompressionInputStream(is, readOptions.getCompression()));
  }
}
