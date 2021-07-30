package org.pipecraft.pipes.sync.source;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.Storage;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.utils.PipeSupplier;

/**
 * Reads data from multiple files in cloud storage under some folder, as if they were concatenated using some predefined order.
 * Files are automatically un-compressed according to their extensions (See {@link Compression}) for supported formats.
 * In contrast to the more general and flexible {@link StorageMultiFileReaderPipe}, here the data is always streamed and not downloaded first,
 * so it may be less efficient in some cases.
 * 
 * @param <B> The bucket's file metadata type in the cloud storage implementation being used
 * 
 * For more features and for general binary files use {@link StorageMultiFileReaderPipe}.
 * 
 * @author Eyal Schneider
 */
public class StorageMultiTxtFileReaderPipe<B> extends CompoundPipe<String> {
  private final Storage<?, B> storage;
  private final String bucketName;
  private final String folderPath;
  private final Charset charset;
  private final int chunkSize;
  private final String fileRegex;
  private final Comparator<B> comparator;
  
  /**
   * Constructor
   * 
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param folderPath The full path of the folder to read the files from
   * @param charset The charset used
   * @param chunkSize The size (in bytes) of each chunk read from storage at once, or 0 for using the default one.
   * @param fileRegex Used for determining which files to read from based on the file name
   * @param comparator A comparator used for defining the order at which file are read
   */
  public StorageMultiTxtFileReaderPipe(Storage<?, B> storage, String bucket, String folderPath, Charset charset, int chunkSize, String fileRegex, Comparator<B> comparator) {
    this.storage = storage;
    this.bucketName = bucket;
    this.folderPath = folderPath;
    this.charset = charset;
    this.chunkSize = chunkSize;
    this.fileRegex = fileRegex;
    this.comparator = comparator;
  }

  /**
   * Constructor
   * 
   * Assumes UTF8 encoding of all files, and doesn't apply any filter on files to read from.
   * 
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param folderPath The full path of the folder to read the files from
   * @param comparator A comparator used for defining the order at which file are read
   */
  public StorageMultiTxtFileReaderPipe(Storage<?, B> storage, String bucket, String folderPath, Comparator<B> comparator) {
    this(storage, bucket, folderPath, StandardCharsets.UTF_8, 0, ".*", comparator);
  }

  /**
   * Constructor
   * 
   * Scans the remote files in lexicographic name order. Performs no filtering, and assumes UTF8 encoding.
   * 
   * @param storage The cloud storage connector
   * @param bucket The bucket to read the file from
   * @param folderPath The full path of the folder to read the files from
   */
  public StorageMultiTxtFileReaderPipe(Storage<?, B> storage, String bucket, String folderPath) {
    this(storage, bucket, folderPath, StandardCharsets.UTF_8, 0, ".*", new LexicographicOrder<>(storage, bucket));
  }

  @Override
  protected Pipe<String> createPipeline() throws PipeException, InterruptedException {
    try {
      List<PipeSupplier<String>> pipeGenList = getPipeGenList();
      return new ConcatPipe<>(pipeGenList);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  private List<PipeSupplier<String>> getPipeGenList() throws IOException {
    Bucket<B> bucket = storage.getBucket(bucketName);
    ArrayList<B> blobs = new ArrayList<>();
    bucket.listFiles(folderPath, fileRegex).forEachRemaining(blobs::add);
    blobs.sort(comparator);
    List<PipeSupplier<String>> res = new ArrayList<>();
    for (B b : blobs) {
      String path = bucket.getPath(b);
      FileReadOptions options = new FileReadOptions().detectCompression(path);
      res.add(() -> new StorageTxtFileReaderPipe(storage, bucketName, path, charset, chunkSize, options));
    }
    
    return res;
  }
  
  private static class LexicographicOrder<T> implements Comparator<T> {
    private final Bucket<T> bucket;

    public LexicographicOrder(Storage<?, T> storage, String bucketName) {
      this.bucket = storage.getBucket(bucketName);
    }

    @Override
    public int compare(T b1, T b2) {
      String path1 = bucket.getPath(b1);
      String path2 = bucket.getPath(b2);
      return path1.compareTo(path2);
    }
  }
}
