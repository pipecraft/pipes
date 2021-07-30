package org.pipecraft.pipes.source.google_cs;

import java.nio.charset.Charset;

import org.pipecraft.infra.storage.google_cs.GoogleStorage;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.storage.Storage;
import org.pipecraft.pipes.sync.source.StorageTxtFileReaderPipe;

/**
 * A source pipe providing the lines of a text file in some {@link Storage}, in a streaming manner. Supports decompression.
 * Note that for larger files {@link GSTxtFileFetcherReaderPipe} may be much faster.
 * 
 * @author Eyal Schneider
 */
public class GSTxtFileReaderPipe extends StorageTxtFileReaderPipe {

  /**
   * Constructor
   *
   * @param gs The storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param chunkSize The size (in bytes) of each chunk read from storage at once, or 0 for using the default one.
   * @param options The file read options
   */
  public GSTxtFileReaderPipe(GoogleStorage gs, String bucket, String path,
      Charset charset, int chunkSize, FileReadOptions options) {
    super(gs, bucket, path, charset, chunkSize, options);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options
   *
   * @param gs The storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   */
  public GSTxtFileReaderPipe(GoogleStorage gs, String bucket, String path, Charset charset) {
    super(gs, bucket, path, charset);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and assumes UTF8
   *
   * @param gs The storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param options The file read options
   */
  public GSTxtFileReaderPipe(GoogleStorage gs, String bucket, String path, FileReadOptions options) {
    super(gs, bucket, path, options);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options, and assumes UTF8
   *
   * @param gs The storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   */
  public GSTxtFileReaderPipe(GoogleStorage gs, String bucket, String path) {
    super(gs, bucket, path);
  }
}