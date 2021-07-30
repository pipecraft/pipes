package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;

/**
 * An {@link ItemDecoder} that decodes objects encoded in avro format.
 * 
 * @author Zacharya Haitin, Eyal Schneider
 */
public class AvroDecoder<T> implements ItemDecoder<T> {
  private final InputStream is;
  private final DatumReader<T> reader;
  private final BinaryDecoder decoder;
  
  /**
   * Constructor.
   * 
   * @param clazz The java class the bytes will be decoded to.
   * @param is The input stream to bound to this decoder. Not expected to be buffered.
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  public AvroDecoder(Class<T> clazz, InputStream is) throws IOException {
    this(ReflectData.get().getSchema(clazz), is, new FileReadOptions());
  }

  /**
   * Private constructor
   * 
   * @param schema The avro schema this decoder works with
   * @param is The input stream to bound to this decoder. Not expected to be buffered.
   * @param options The read options defining how to handle the input stream
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  private AvroDecoder(Schema schema , InputStream is, FileReadOptions options) throws IOException {
    reader = new ReflectDatumReader<>(schema);
    DecoderFactory factory = new DecoderFactory();
    factory.configureDecoderBufferSize(options.getBufferSize());
    this.is = FileUtils.getCompressionInputStream(is, options.getCompression());
    this.decoder = factory.binaryDecoder(is, null);
  }

  @Override
  public T decode() throws IOException {
    if (decoder.isEnd()) {
      return null;
    }

    return reader.read(null, decoder);
  }
  
  @Override
  public void close() throws IOException {
    is.close();
  }

  /**
   * @param clazz The java class representing the decoded items' data type
   * @return A decoder factory producing avro decoders based on the given type
   */
  public static <R> org.pipecraft.pipes.serialization.DecoderFactory<R> getFactory(Class<R> clazz) {
    return new Factory<>(clazz);
  }
  
  private static class Factory <R> implements org.pipecraft.pipes.serialization.DecoderFactory<R> {
    private final Schema schema;
    
    public Factory(Class<R> clazz) {
      schema = ReflectData.get().getSchema(clazz);
    }
    
    @Override
    public ItemDecoder<R> newDecoder(InputStream is, FileReadOptions options) throws IOException {
      return new AvroDecoder<>(schema, is, options);
    }
  }
}
