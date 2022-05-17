package org.pipecraft.pipes.serialization;

import java.io.IOException;
import java.io.OutputStream;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;

/**
 * An {@link ItemEncoder} that encodes objects in Protocol Buffers format (sequential, delimited proto objects).
 *
 * @param <T> The data type of the items to encode. Must be a protobuf generated type (i.e. must extend {@link com.google.protobuf.Message}).
 *
 * @author Eyal Schneider
 */
public class ProtobufEncoder<T extends com.google.protobuf.Message> implements ItemEncoder<T> {
  private final OutputStream os;

  /**
   * Constructor
   *
   * @param os The output stream to bound to this encoder. Not expected to be buffered.
   * @throws IOException In case of an IO error while preparing to write to the stream
   */
  public ProtobufEncoder(OutputStream os) throws IOException {
    this(os, new FileWriteOptions());
  }

  /**
   * Constructor
   *
   * @param os The output stream to bound to this encoder. Not expected to be buffered.
   * @param options The write options defining how to handle the output stream
   * @throws IOException In case of an IO error while preparing to write to the stream
   */
  public ProtobufEncoder(OutputStream os, FileWriteOptions options) throws IOException {
    this.os = FileUtils.getOutputStream(os, options);
  }

  @Override
  public void encode(T item) throws IOException {
    item.writeDelimitedTo(os);
  }

  @Override
  public void close() throws IOException {
    os.close();
  }

  /**
   * @return A decoder factory producing protobuf decoders based on the given type
   */
  public static <R extends com.google.protobuf.Message> EncoderFactory<R> getFactory() {
    return new Factory<>();
  }

  private static class Factory <R extends com.google.protobuf.Message> implements EncoderFactory<R> {
    @Override
    public ItemEncoder<R> newEncoder(OutputStream os, FileWriteOptions options) throws IOException {
      return new ProtobufEncoder<>(os, options);
    }
  }
}
