package org.pipecraft.pipes.async.source;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pipecraft.pipes.async.inter.AsyncCompoundPipe;
import org.pipecraft.pipes.async.inter.SyncToAsyncPipe;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.multi.MultiFileReaderUtils;
import org.pipecraft.pipes.utils.multi.StorageMultiFileReaderConfig;

/**
 * An async source pipe reading multiple remote binary files from the cloud.
 * Supports file filtering and automatic sharding of the data.
 * 
 * @param <T> The output item data type
 * @param <B> The remote file metadata object type
 *
 * @author Eyal Schneider
 */
public class AsyncStorageMultiFileReaderPipe<T, B> extends AsyncCompoundPipe<T> {
  private final StorageMultiFileReaderConfig<T, B> config;

  /**
   * Constructor
   * 
   * @param config The multi reader config
   */
  public AsyncStorageMultiFileReaderPipe(StorageMultiFileReaderConfig<T, B> config) {
    this.config = config;
  }
  
  @Override
  protected AsyncPipe<T> createPipeline() throws IOException {
    try {
      // Determine all files to read
      Collection<B> files = MultiFileReaderUtils.getAllRemoteFilesToRead(config);
      Bucket<B> bucket = config.getBucket();

      // Create pipe suppliers
      List<PipeSupplier<T>> pipesSuppliers;
      if (config.isDownloadFirst()) { // Download and then read
        pipesSuppliers = MultiFileReaderUtils.downloadAndGetReadPipes(files, bucket, config);
      } else { // Streaming
        pipesSuppliers = new ArrayList<>(files.size());
        for (B f : files) {
          pipesSuppliers.add(() -> {
            SizedInputStream is = bucket.getAsStream(f);
            return config.getPipeSupplier().get(is, f);
          });
        }
      }
      
      // Concat
      return new SyncToAsyncPipe<>(pipesSuppliers, config.getThreadNum());
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
  }
}
