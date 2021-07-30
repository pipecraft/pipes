package org.pipecraft.pipes.sync.source;

import org.pipecraft.infra.concurrent.FailableInterruptibleSupplier;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.terminal.TxtFileWriterPipe;

/**
 * A source pipe reading lines from a given local text file or input stream
 * containing text content.
 * Each line produces a single pipe item. Supports decompression.
 * @see TxtFileWriterPipe
 *
 * @author Eyal Schneider
 * 
 */
public class TxtFileReaderPipe extends InputStreamPipe<String> {
  private final FailableInterruptibleSupplier<SizedInputStream, IOException> isCreator;
  private final Charset charset;
  private final int bufferSize;
  private BufferedReader reader;
  private String next;
  

  /**
   * Constructor
   *
   * @param isCreator The creator of the stream to read from. Expected to be the raw input stream (without decompression or buffering layers), 
   * but should be initialized with expected size (see {@link SizedInputStream}).
   * The size should be either null if unknown, or it should be the exact number of stream bytes.
   * @param charset The charset to use
   * @param bufferSizeBytes The requires buffer size
   * @param compression Compression type
   */
  public TxtFileReaderPipe(FailableInterruptibleSupplier<SizedInputStream, IOException> isCreator, Charset charset, int bufferSizeBytes, Compression compression) {
    super(0, compression); // We disable input stream buffering because we have a buffering in the reader layer anyway
    this.isCreator = isCreator;
    this.charset = charset;
    this.bufferSize = bufferSizeBytes;
  }

  /**
   * Constructor
   *
   * @param is The input stream to read from. Expected to be the raw input stream (without decompression or buffering layers).
   * @param charset The charset to use
   * @param bufferSizeBytes The requires buffer size
   * @param compression Compression type
   */
  public TxtFileReaderPipe(SizedInputStream is, Charset charset, int bufferSizeBytes, Compression compression) {
    this(() -> is, charset, bufferSizeBytes, compression); // We disable input stream buffering because we have a buffering in the reader layer anyway
  }

  /**
   * Constructor
   *
   * @param f The text file to read from
   * @param charset The charset to use
   * @param options The file reading options
   */
  public TxtFileReaderPipe(File f, Charset charset, FileReadOptions options) {
    this(() -> new SizedInputStream(new FileInputStream(f), f.length()), charset, options.getBufferSize(), options.getCompression());
  }

  /**
   * Constructor
   * 
   * Detects the compression type by the filename extension
   *
   * @param f The text file to read from. Compression is inferred from file extension.
   * @param charset The charset to use
   */
  public TxtFileReaderPipe(File f, Charset charset) {
    this(f, charset, new FileReadOptions().detectCompression(f.getName()));
  }

  /**
   * Constructor
   * 
   * Detects the compression type by the filename extension
   *
   * @param f The text file to read from. Assumed to be UTF8. Compression is inferred from file extension
   */
  public TxtFileReaderPipe(File f) {
    this(f, StandardCharsets.UTF_8, new FileReadOptions().detectCompression(f.getName()));
  }

  /**
   * Constructor
   *
   * @param f The text file to read from. Assumed to be UTF8.
   * @param options The file reading options
   */
  public TxtFileReaderPipe(File f, FileReadOptions options) {
    this(f, StandardCharsets.UTF_8, options);
  }

  @Override
  public String next() throws PipeException, InterruptedException {
    String toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public String peek() {
    return next;
  }

  @Override
  protected SizedInputStream createInputStream() throws IOException, InterruptedException {
    return isCreator.get();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    super.start();
    this.reader = toReader(getInputStream(), charset, bufferSize);
    prepareNext();
  }

  public void close() throws IOException {
    FileUtils.close(reader);
  }
  
  private void prepareNext() throws IOPipeException {
    try {
      next = reader.readLine();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  private static BufferedReader toReader(InputStream is, Charset charset, int bufferSize) {
    return new BufferedReader(new InputStreamReader(is, charset), bufferSize);
  }
  
}
