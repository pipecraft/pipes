package org.pipecraft.pipes.terminal.google_cs;

import java.nio.charset.Charset;
import org.pipecraft.infra.storage.google_cs.GoogleStorage;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.source.google_cs.GSTxtFileReaderPipe;
import org.pipecraft.pipes.terminal.StorageTxtFileWriterPipe;

/**
 * A terminal pipe writing text lines from the input pipe into a remote GoogleStorage file. Supports compression.
 * @see GSTxtFileReaderPipe
 * 
 * @author Eyal Schneider
 *
 */
public class GSTxtFileWriterPipe extends StorageTxtFileWriterPipe {
  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param gs The google storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param chunkSize The size (in bytes) of each chunk written to GS at once, or 0 for using the default one.
   * @param charset The charset to use
   * @param options The file writing options. Note that append and temp aren't supported by GS, so these flag are ignored.
   */
  public GSTxtFileWriterPipe(Pipe<String> input, GoogleStorage gs, String bucket, String path, Charset charset, int chunkSize, FileWriteOptions options) {
    super(input, gs, bucket, path, charset, chunkSize, options);
  }

  /**
   * Constructor
   * 
   * Uses the default write options and chunk size
   * @param input The input pipe
   * @param gs The google storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param charset The charset to use
   */
  public GSTxtFileWriterPipe(Pipe<String> input, GoogleStorage gs, String bucket, String path, Charset charset) {
    super(input, gs, bucket, path, charset);
  }

  /**
   * Constructor
   * 
   * Uses default chunk size and UTF8
   * @param input The input pipe
   * @param gs The google storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param options The file writing options. Note that append isn't supported by GS, so this flag is ignored.
   */
  public GSTxtFileWriterPipe(Pipe<String> input, GoogleStorage gs, String bucket, String path, FileWriteOptions options) {
    super(input, gs, bucket, path, options);
  }

  /**
   * Constructor
   * 
   * Uses default write options and chunk size, and assumes UTF8
   * @param input The input pipe
   * @param gs The google storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   */
  public GSTxtFileWriterPipe(Pipe<String> input, GoogleStorage gs, String bucket, String path) {
    super(input, gs, bucket, path);
  }
}
