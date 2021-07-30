package org.pipecraft.pipes.sync.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.pipecraft.infra.concurrent.FailableInterruptibleSupplier;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.serialization.ItemDecoder;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.serialization.DecoderFactory;

/**
 * A source pipe reading items from a given binary input (file or input stream).
 * 
 * @author Eyal Schneider
 */
public class BinInputReaderPipe<T> extends InputStreamPipe<T> {

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  private final FailableInterruptibleSupplier<SizedInputStream, IOException> isCreator;
  private final DecoderFactory<T> decoderFactory;
  private final FileReadOptions readOptions;
  private ItemDecoder<T> decoder;
  private T next;

  /**
   * Constructor
   * 
   * @param isCreator The creator of the input stream. Expected to be the raw input stream (without decompression or buffering layers), 
   * but should be initialized with expected size (see {@link SizedInputStream}).
   * The size should be either null if unknown, or it should be the exact number of stream bytes. 
   * @param decoderFactory The decoder of binary values to pipe items. 
   * @param bufferSize Buffer size to use on the input stream
   * @param compression Input stream compression
   */
  public BinInputReaderPipe(FailableInterruptibleSupplier<SizedInputStream, IOException> isCreator, DecoderFactory<T> decoderFactory, int bufferSize, 
      Compression compression) {
    super(0, Compression.NONE); // Buffer size and compression are applied later when initializing the decoder
    this.isCreator = isCreator;
    this.readOptions = new FileReadOptions().buffer(bufferSize).setCompression(compression);
    this.decoderFactory = decoderFactory;
  }

  /**
   * Constructor
   * 
   * @param is Input stream to read items from. Expected to be the raw input stream (without decompression or buffering layers).
   * @param decoderFactory The decoder of binary values to pipe items. 
   * @param bufferSize Buffer size to use on the input stream
   * @param compression Input stream compression
   */
  public BinInputReaderPipe(InputStream is, DecoderFactory<T> decoderFactory, int bufferSize, long sizeBytes, 
      Compression compression) {
    this(() -> new SizedInputStream(is, sizeBytes), decoderFactory, bufferSize, compression);
  }

  /**
   * Constructor
   * 
   * @param is The {@link SizedInputStream} to read items from. Expected to be the raw input stream (without decompression or buffering layers).
   * @param decoderFactory The decoder of binary values to pipe items. 
   * @param readOptions The reading options
   */
  public BinInputReaderPipe(SizedInputStream is, DecoderFactory<T> decoderFactory, FileReadOptions readOptions) {
    this(() -> is, decoderFactory, readOptions.getBufferSize(), readOptions.getCompression());
  }

  /**
   * Constructor
   *
   * @param inputFile Input file to read items from
   * @param options The file read options
   * @param decoderFactory The decoder of binary values to pipe items
   */
  public BinInputReaderPipe(File inputFile, FileReadOptions options, DecoderFactory<T> decoderFactory) {
    this(() -> new SizedInputStream(new FileInputStream(inputFile), inputFile.length()), decoderFactory, options.getBufferSize(), options.getCompression());
  }

  /**
   * Constructor
   * 
   * @param inputFile The file to read items from
   * @param decoderFactory The decoder of binary values to pipe items
   */
  public BinInputReaderPipe(File inputFile, DecoderFactory<T> decoderFactory) {
    this(() -> new SizedInputStream(new FileInputStream(inputFile), inputFile.length()), decoderFactory, DEFAULT_BUFFER_SIZE, Compression.NONE);
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  protected SizedInputStream createInputStream() throws IOException, InterruptedException {
    return isCreator.get();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    super.start();
    try {
      decoder = decoderFactory.newDecoder(getInputStream(), readOptions);
      prepareNext();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  private void prepareNext() throws PipeException {
    try {
      next = decoder.decode();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
