package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;

/**
 * An {@link ItemEncoder} that encode any java class to bytes using avro.
 * 
 * @author Zacharya Haitin
 */
public class AvroEncoder<T> implements ItemEncoder<T> {
  private final DatumWriter<T> writer;
  private final BinaryEncoder encoder;
  private final OutputStream os;

  /**
   * Constructor
   * 
   * @param clazz The java class representing the type to encode
   * @param os The output stream this encoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @throws IOException In case of an IO error while preparing to write to the output stream
   */
  public AvroEncoder(Class<T> clazz, OutputStream os) throws IOException {
    this(ReflectData.get().getSchema(clazz), os, new FileWriteOptions());
  }

  /**
   * Private constructor
   * 
   * @param schema The avro schema this encoder works with
   * @param os The output stream this encoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param options the write options to apply on the output stream
   * @throws IOException In case of an IO error while preparing to write to the output stream
   */
  private AvroEncoder(Schema schema, OutputStream os, FileWriteOptions options) throws IOException {
    writer = new ReflectDatumWriter<>(schema);
    EncoderFactory factory = new EncoderFactory();
    factory.configureBufferSize(options.getBufferSize());
    this.os = FileUtils.getCompressionOutputStream(os, options.getCompression(), options.getCompressionLevel());
    encoder = factory.binaryEncoder(os, null);
  }

  @Override
  public void close() throws IOException {
    encoder.flush();
    os.close();
  }

  @Override
  public void encode(T item) throws IOException {
    writer.write(item, encoder);
  }

  /**
   * @param clazz The java class representing the encoded items' data type
   * @return An encoder factory producing avro encoders based on the given type
   */
  public static <R> org.pipecraft.pipes.serialization.EncoderFactory<R> getFactory(Class<R> clazz) {
    return new Factory<>(clazz);
  }
  
  private static class Factory <R> implements org.pipecraft.pipes.serialization.EncoderFactory<R> {
    private final Schema schema;
    
    public Factory(Class<R> clazz) {
      schema = ReflectData.get().getSchema(clazz);
    }
    
    @Override
    public ItemEncoder<R> newEncoder(OutputStream os, FileWriteOptions options) throws IOException {
      return new AvroEncoder<>(schema, os, options);
    }
  }

}
