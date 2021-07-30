package org.pipecraft.pipes.sync.source;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.PathUtils;
import org.pipecraft.infra.storage.Storage;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe providing the lines of a text file from cloud storage. Supports decompression.
 * In contrast to the streaming nature of {@link StorageTxtFileReaderPipe}, this implementation downloads the file completely first.
 * This approach may be much faster for large files, since the download is optimized (sliced) and reading from local disk is usually faster than network read.
 * 
 * @author Eyal Schneider
 */
public class StorageTxtFileFetcherReaderPipe extends CompoundPipe<String> {

  private static final int DEFAULT_CHUNK_SIZE = 0;
  private static final FileReadOptions DEFAULT_FILE_READ_OPTIONS = new FileReadOptions();
  
  private final Storage<?, ?> storage;
  private final String bucketName;
  private final String path;
  private final Charset charset;
  private final int chunkSize;
  private final FileReadOptions readOptions;
  private final File tempFolder;
  private File tmpFile;

  /**
   * Constructor
   * 
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param chunkSize The size (in bytes) of each chunk read from storage at once, or 0 for using the default one.
   * @param options The file read options
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work. 
   */
  public StorageTxtFileFetcherReaderPipe(Storage<?, ?> storage, String bucket, String path, Charset charset, int chunkSize, FileReadOptions options, File tempFolder) {
    this.storage = storage;
    this.bucketName = bucket;
    this.path = path;
    this.charset = charset;
    this.chunkSize = chunkSize;
    this.readOptions = options;
    this.tempFolder = tempFolder;
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options
   *
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public StorageTxtFileFetcherReaderPipe(Storage<?, ?> storage, String bucket, String path, Charset charset, File tempFolder) {
    this(storage, bucket, path, charset, DEFAULT_CHUNK_SIZE, DEFAULT_FILE_READ_OPTIONS, tempFolder);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options, and assumes UTF8
   *
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public StorageTxtFileFetcherReaderPipe(Storage<?, ?> storage, String bucket, String path, File tempFolder) {
    this(storage, bucket, path, StandardCharsets.UTF_8, DEFAULT_CHUNK_SIZE, DEFAULT_FILE_READ_OPTIONS, tempFolder);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and assumes UTF8
   *
   * @param storage The cloud torage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param options The file read options
   * @param tempFolder A folder where to place the temporary file for the download. The file is deleted by the pipe at the end of work.
   */
  public StorageTxtFileFetcherReaderPipe(Storage<?, ?> storage, String bucket, String path, FileReadOptions options, File tempFolder) {
    this(storage, bucket, path, StandardCharsets.UTF_8, DEFAULT_CHUNK_SIZE, options, tempFolder);
  }

  @Override
  protected Pipe<String> createPipeline() throws PipeException, InterruptedException {
    // Download the file
    try {
      Bucket<?> bucket = storage.getBucket(bucketName);
      tmpFile = FileUtils
          .createTempFile("downloaded_" + PathUtils.getLastPathPart(path), ".tmp", tempFolder);
      bucket.getSliced(path, tmpFile, chunkSize);
      
      return new TxtFileReaderPipe(tmpFile, charset, readOptions);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    tmpFile.delete();
  }
}
