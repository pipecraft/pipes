package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.Storage;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A terminal pipe writing text lines from the input pipe into a remote cloud storage file. Supports compression.
 *
 * Note that this class makes use of upload using IO streams, which is an optional feature
 * of {@link Bucket} class. Therefore, some types of {@link Storage} won't be supported here.
 * 
 * @author Eyal Schneider
 */
public class StorageTxtFileWriterPipe extends TerminalPipe {
  private final Pipe<String> input;
  private final Storage<?,?> storage;
  private final String bucketName;
  private final String path;
  private final Charset charset;
  private final int chunkSize;
  private final FileWriteOptions options;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param storage The cloud storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param chunkSize The size (in bytes) of each chunk written to GS at once, or 0 for using the default one.
   * @param charset The charset to use
   * @param options The file writing options. Note that append and temp aren't supported by GS, so these flag are ignored.
   */
  public StorageTxtFileWriterPipe(Pipe<String> input, Storage<?, ?> storage, String bucket, String path, Charset charset, int chunkSize, FileWriteOptions options) {
    this.input = input;
    this.storage = storage;
    this.bucketName = bucket;
    this.path = path;
    this.charset = charset;
    this.chunkSize = chunkSize;
    this.options = options;
  }

  /**
   * Constructor
   * 
   * Uses the default write options and chunk size
   * @param input The input pipe
   * @param gs The cloud storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param charset The charset to use
   */
  public StorageTxtFileWriterPipe(Pipe<String> input, Storage<?, ?> gs, String bucket, String path, Charset charset) {
    this(input, gs, bucket, path, charset, 0, new FileWriteOptions());
  }

  /**
   * Constructor
   * 
   * Uses default chunk size and UTF8
   * @param input The input pipe
   * @param storage The cloud storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   * @param options The file writing options. Note that append isn't supported by GS, so this flag is ignored.
   */
  public StorageTxtFileWriterPipe(Pipe<String> input, Storage<?, ?> storage, String bucket, String path, FileWriteOptions options) {
    this(input, storage, bucket, path, StandardCharsets.UTF_8, 0, options);
  }

  /**
   * Constructor
   * 
   * Uses default write options and chunk size, and assumes UTF8
   * @param input The input pipe
   * @param storage The cloud storage connector
   * @param bucket The bucket to write to
   * @param path The full path (excluding the bucket) of the target file
   */
  public StorageTxtFileWriterPipe(Pipe<String> input, Storage<?, ?> storage, String bucket, String path) {
    this(input, storage, bucket, path, StandardCharsets.UTF_8, 0, new FileWriteOptions());
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    BufferedWriter bw = null;
    try {
      Bucket<?> bucket = storage.getBucket(bucketName);
      OutputStream os = bucket.getOutputStream(path, chunkSize);
      bw = FileUtils.getWriter(os, charset, options);

      input.start();
      String item;
      while ((item = input.next()) != null) {
        bw.write(item);
        bw.newLine();
      }
      bw.flush();
    } catch (IOException e) {
      throw new IOPipeException(e);
    } finally {
      FileUtils.closeSilently(bw);
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
