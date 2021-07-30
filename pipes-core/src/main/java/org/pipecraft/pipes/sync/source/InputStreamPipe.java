package org.pipecraft.pipes.sync.source;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.io.ThreadSafeCountingInputStream;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A base class for source pipes based on some input stream.
 * Handles progress tracking.
 * Subclasses should override start() method, call super.start() and then perform specific pre-processing
 * on the input stream.
 * 
 * @param <T> The Pipe's item data type
 *
 * @author Eyal Schneider
 */
public abstract class InputStreamPipe <T> implements Pipe<T> {
  private final int bufferSizeBytes;
  private final Compression compression;
  
  private ThreadSafeCountingInputStream inputByteCounterStream;
  private InputStream is;
  private Long size;
  
  /**
   * Constructor
   * 
   * @param bufferSizeBytes The requires buffer size, if <= 0 we don't use a buffer
   * @param compression Compression type
   */
  public InputStreamPipe(int bufferSizeBytes, Compression compression) {
    this.bufferSizeBytes = bufferSizeBytes;
    this.compression = compression;    
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      SizedInputStream sizedIs = createInputStream();
      size = sizedIs.getSize();
      this.inputByteCounterStream = new ThreadSafeCountingInputStream(sizedIs); // For progress tracking
      InputStream cis = FileUtils.getCompressionInputStream(inputByteCounterStream, compression);
      this.is = bufferSizeBytes > 0 ? new BufferedInputStream(cis, bufferSizeBytes) : cis;
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    FileUtils.close(is);
  }

  /**
   * @return A new input stream based on ctor arguments. Called once in the start() method. 
   * Expected to be the raw input stream (without decompression or buffering layers), 
   * but should be initialized with expected size (see {@link SizedInputStream}).
   * The size should be null if unknown.
   * @throws IOException (Will be wrapped by IOPipeException)
   * @throws IOPipeException (Are propagated without wrapping)
   * @throws InterruptedException In case that the operation is interrupted
   */
  protected abstract SizedInputStream createInputStream() throws IOException, IOPipeException, InterruptedException;

  /**
   * @return The input stream initialized according to the ctor parameters.
   * Subclasses may use this stream in their start method after calling super.start().
   */
  protected InputStream getInputStream() {
    return is;
  }

  /**
   * @return The expected input stream size, when created.
   * Subclasses may use this stream in their start method after calling super.start().
   */
  protected Long getInputStreamSize() {
    return size;
  }

  @Override
  public float getProgress() {
    if (size == null) { // If no total size provided, use simple 0% or 100% progress logic.
      try {
        return peek() == null ? 1.0f : 0.0f;
      } catch (PipeException e) {
        return 0.0f; // Assuming that the caller will meet the error anyway during iteration.
      }
    }
    
    if (size == 0) {
      return 1.0f;
    }
    return getBytesRead() / (float) size;
  }
  
  private long getBytesRead() {
    return inputByteCounterStream.getCount();
  }

}
