package org.pipecraft.pipes.source.google_cs;

import java.io.File;
import java.nio.charset.Charset;

import org.pipecraft.infra.storage.google_cs.GoogleStorage;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.sync.source.StorageTxtFileFetcherReaderPipe;

/**
 * A source pipe providing the lines of a text file from Google-Storage. Supports decompression.
 * In contrast to the streaming nature of {@link GSTxtFileReaderPipe}, this implementation downloads the file completely first.
 * This approach may be much faster for large files, since the download is optimized (sliced) and reading from local disk is usually faster than network read.
 *
 * @author Eyal Schneider
 */
public class GSTxtFileFetcherReaderPipe extends StorageTxtFileFetcherReaderPipe {
  /**
   * Constructor
   * 
   * @param gs The Google-Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param chunkSize The size (in bytes) of each chunk read from GS at once, or 0 for using the default one.
   * @param options The file read options
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work. 
   */
  public GSTxtFileFetcherReaderPipe(GoogleStorage gs, String bucket, String path, Charset charset, int chunkSize, FileReadOptions options, File tempFolder) {
    super(gs, bucket, path, charset, chunkSize, options, tempFolder);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options
   *
   * @param gs The Google-Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public GSTxtFileFetcherReaderPipe(GoogleStorage gs, String bucket, String path, Charset charset, File tempFolder) {
    super(gs, bucket, path, charset, tempFolder);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options, and assumes UTF8
   *
   * @param gs The Google-Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public GSTxtFileFetcherReaderPipe(GoogleStorage gs, String bucket, String path, File tempFolder) {
    super(gs, bucket, path, tempFolder);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and assumes UTF8
   *
   * @param gs The Google-Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param options The file read options
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public GSTxtFileFetcherReaderPipe(GoogleStorage gs, String bucket, String path, FileReadOptions options, File tempFolder) {
    super(gs, bucket, path, options, tempFolder);
  }
}
