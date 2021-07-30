package org.pipecraft.pipes.source.google_cs;

import java.nio.charset.Charset;
import java.util.Comparator;

import com.google.cloud.storage.Blob;
import org.pipecraft.infra.storage.google_cs.GoogleStorage;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.pipes.sync.source.StorageMultiTxtFileReaderPipe;

/**
 * Reads data from multiple files in Google-Storage under some folder, as if they were concatenated using some predefined order.
 * Files are automatically un-compressed according to their extensions (See {@link Compression}) for supported formats.
 * 
 * @author Eyal Schneider
 */
public class GSMultiTxtFileReaderPipe extends StorageMultiTxtFileReaderPipe<Blob> {

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
  public GSMultiTxtFileReaderPipe(GoogleStorage storage, String bucket,
      String folderPath, Charset charset, int chunkSize, String fileRegex, Comparator<Blob> comparator) {
    super(storage, bucket, folderPath, charset, chunkSize, fileRegex, comparator);
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
  public GSMultiTxtFileReaderPipe(GoogleStorage storage, String bucket, String folderPath, Comparator<Blob> comparator) {
    super(storage, bucket, folderPath, comparator);
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
  public GSMultiTxtFileReaderPipe(GoogleStorage storage, String bucket, String folderPath) {
    super(storage, bucket, folderPath);
  }
}