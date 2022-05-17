package org.pipecraft.pipes.serialization;

import com.google.protobuf.ExtensionRegistryLite;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * An {@link ItemDecoder} that decodes objects encoded in Protocol Buffers format (sequential, delimited proto objects).
 *
 * @param <T> The data type of the decoded items. Must be a protobuf generated type (i.e. must extend {@link com.google.protobuf.Message}).
 *
 * @author Eyal Schneider
 */
public class ProtobufDecoder<T extends com.google.protobuf.Message> implements ItemDecoder<T> {
  private static final ExtensionRegistryLite EMPTY_REGISTRY = ExtensionRegistryLite.getEmptyRegistry();

  private final Class<T> clazz;
  private final Method parseMethod;
  private final InputStream is;
  private final ExtensionRegistryLite registry;

  /**
   * Constructor
   *
   * @param clazz The class identifying the type of the decoded items
   * @param is The input stream to bound to this decoder. Not expected to be buffered.
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  public ProtobufDecoder(Class<T> clazz, InputStream is) throws IOException {
    this(clazz, is, new FileReadOptions(), EMPTY_REGISTRY);
  }

  /**
   * Constructor
   *
   * @param clazz The class identifying the type of the decoded items
   * @param is The input stream to bound to this decoder. Not expected to be buffered.
   * @param options The read options defining how to handle the input stream
   * @param registry An extension registry for supporting user-defined fields not specified in the protobuf class specification.
   * @throws IOException In case of an IO error while preparing to read from the input stream
   */
  public ProtobufDecoder(Class<T> clazz , InputStream is, FileReadOptions options, ExtensionRegistryLite registry) throws IOException {
    this.clazz = clazz;
    this.registry = registry;
    try {
      parseMethod = clazz.getMethod("parseDelimitedFrom", InputStream.class, ExtensionRegistryLite.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Couldn't find or use the parseFrom() method for the proto type: " + clazz.getSimpleName());
    }
    this.is = FileUtils.getInputStream(is, options);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T decode() throws IOException, ValidationPipeException {
    try {
      return (T) parseMethod.invoke(clazz, is, registry);
    } catch (InvocationTargetException e) {
      throw new ValidationPipeException(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    is.close();
  }

  /**
   * @param clazz The class identifying the type of the decoded items
   * @return A decoder factory producing protobuf decoders based on the given type
   */
  public static <R extends com.google.protobuf.Message> DecoderFactory<R> getFactory(Class<R> clazz) {
    return new Factory<>(clazz, EMPTY_REGISTRY);
  }

  /**
   * @param clazz The class identifying the type of the decoded items
   * @param registry An extension registry for supporting user-defined fields not specified in the protobuf class specification.
   * @return A decoder factory producing protobuf decoders based on the given type
   */
  public static <R extends com.google.protobuf.Message> DecoderFactory<R> getFactory(Class<R> clazz, ExtensionRegistryLite registry) {
    return new Factory<>(clazz, registry);
  }

  private static class Factory <R extends com.google.protobuf.Message> implements DecoderFactory<R> {
    private final Class<R> clazz;
    private final ExtensionRegistryLite registry;

    public Factory(Class<R> clazz, ExtensionRegistryLite registry) {
      this.clazz = clazz;
      this.registry = registry;
    }

    @Override
    public ItemDecoder<R> newDecoder(InputStream is, FileReadOptions options) throws IOException {
      return new ProtobufDecoder<>(clazz, is, options, registry);
    }
  }
}
