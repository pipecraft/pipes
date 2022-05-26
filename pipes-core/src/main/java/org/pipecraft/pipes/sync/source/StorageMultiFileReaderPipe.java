package org.pipecraft.pipes.sync.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.multi.LocalMultiFileReaderConfig;
import org.pipecraft.pipes.utils.multi.MultiFileReaderUtils;
import org.pipecraft.pipes.utils.multi.StorageMultiFileReaderConfig;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe reading multiple remote binary files from the cloud.
 * Supports file filtering and ordering, as well as automatic sharding of the data.
 * 
 * @param <T> The output item data type
 * @param <B> The remote file metadata object type
 *
 * @author Eyal Schneider
 */
public class StorageMultiFileReaderPipe<T, B> extends CompoundPipe<T> {
  private final StorageMultiFileReaderConfig<T, B> config;

  /**
   * Constructor
   * 
   * @param config The multi reader config
   */
  public StorageMultiFileReaderPipe(StorageMultiFileReaderConfig<T, B> config) {
    if (config.getBucket() == null) {
      throw new IllegalArgumentException("Missing storage bucket in " + LocalMultiFileReaderConfig.class.getSimpleName());
    }
    this.config = config;
  }
  
  @Override
  protected Pipe<T> createPipeline() throws PipeException, InterruptedException {
    try {
      // Determine all files to read
      Collection<B> filesToRead = MultiFileReaderUtils.getAllRemoteFilesToRead(config);
      
      // Sort (For storage case we always have some ordering, possible the default lexicographic one)
      List<B> sortedFiles = new ArrayList<>(filesToRead);
      Bucket<B> bucket = config.getBucket();
      sortedFiles.sort(config.getFileOrder());

      // Create pipe suppliers
      List<PipeSupplier<T>> pipesSuppliers;
      if (config.isDownloadFirst()) { // Download and then read
        pipesSuppliers = MultiFileReaderUtils.downloadAndGetReadPipes(sortedFiles, bucket, config);
      } else { // Streaming
        pipesSuppliers = new ArrayList<>();
        for (B f : sortedFiles) {
          pipesSuppliers.add(() -> {
            SizedInputStream is = bucket.getAsStream(f);
            return config.getPipeSupplier().get(is, f);
          });
        }
      }
      
      // Concat
      return new ConcatPipe<>(pipesSuppliers);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
