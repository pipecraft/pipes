package org.pipecraft.pipes.sync.source;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.Storage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe providing the lines of a text file in some {@link Storage}, in a streaming manner. Supports decompression.
 * Note that for larger files {@link StorageTxtFileFetcherReaderPipe} may be much faster.
 *
 * @author Eyal Schneider
 */
public class StorageTxtFileReaderPipe extends InputStreamPipe<String> {
  private static final int DEFAULT_CHUNK_SIZE = 0;
  private static final FileReadOptions DEFAULT_FILE_READ_OPTIONS = new FileReadOptions();
  
  private final Charset charset;
  private final Storage<?, ?> storageConnector;
  private final String bucketName;
  private final String path;
  private final int chunkSize;
  private final FileReadOptions readOptions;

  private BufferedReader reader;
  private String next;

  /**
   * Constructor
   *
   * @param storageConnector The Storage connector
   * @param bucketName The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   * @param chunkSize The size (in bytes) of each chunk read from storage at once, or 0 for using the default one.
   * @param options The file read options
   */
  public StorageTxtFileReaderPipe(Storage<?, ?> storageConnector, String bucketName, String path, Charset charset, int chunkSize, FileReadOptions options) {
    super(0, options.getCompression()); // The reader in next line adds buffering anyway, so we don't need it in the input stream level
    this.storageConnector = storageConnector;
    this.bucketName = bucketName;
    this.path = path;
    this.charset = charset;
    this.chunkSize = chunkSize;
    this.readOptions = options;
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options
   *
   * @param storageConnector The Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket
   * @param charset The charset used
   */
  public StorageTxtFileReaderPipe(Storage<?, ?> storageConnector, String bucket, String path, Charset charset) {
    this(storageConnector, bucket, path, charset, DEFAULT_CHUNK_SIZE, DEFAULT_FILE_READ_OPTIONS);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and default read options, and assumes UTF8
   *
   * @param storageConnector The Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   */
  public StorageTxtFileReaderPipe(Storage<?, ?> storageConnector, String bucket, String path) {
    this(storageConnector, bucket, path, StandardCharsets.UTF_8, DEFAULT_CHUNK_SIZE, DEFAULT_FILE_READ_OPTIONS);
  }

  /**
   * Constructor
   *
   * Uses the default read chunk size and assumes UTF8
   *
   * @param storageConnector The Storage connector
   * @param bucket The bucket to read the file from
   * @param path The full path of the text file inside the bucket. The file is assumed to be UTF8.
   * @param options The file read options
   */
  public StorageTxtFileReaderPipe(Storage<?, ?> storageConnector, String bucket, String path, FileReadOptions options) {
    this(storageConnector, bucket, path, StandardCharsets.UTF_8, DEFAULT_CHUNK_SIZE, options);
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
    Bucket<?> bucket = storageConnector.getBucket(bucketName);
    return bucket.getAsStream(path, chunkSize);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    super.start();
    this.reader = new BufferedReader(new InputStreamReader(getInputStream(), charset), readOptions.getBufferSize());
    prepareNext();
  }

  @Override
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
}
