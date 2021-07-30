package org.pipecraft.pipes.async.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.inter.AsyncCompoundPipe;
import org.pipecraft.pipes.async.inter.SyncToAsyncPipe;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.sync.source.MultiFileReaderPipe;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.multi.LocalMultiFileReaderConfig;
import org.pipecraft.pipes.utils.multi.MultiFileReaderUtils;

/**
 * An async source pipe reading multiple local binary files.
 * Supports file filtering and automatic sharding of the data.
 * 
 * This pipe is the async version of {@link MultiFileReaderPipe}.
 *
 * @param <T> The output item data type
 *
 * @author Eyal Schneider
 */
public class AsyncMultiFileReaderPipe<T> extends AsyncCompoundPipe<T> {
  private final LocalMultiFileReaderConfig<T> config;

  /**
   * Constructor
   * 
   * @param config The multi reader config
   */
  public AsyncMultiFileReaderPipe(LocalMultiFileReaderConfig<T> config) {
    this.config = config;
  }
  
  @Override
  protected AsyncPipe<T> createPipeline() throws IOException {
    Collection<File> filesToRead = MultiFileReaderUtils.getAllLocalFilesToRead(config);
       
    List<PipeSupplier<T>> pipesSuppliers = new ArrayList<>();
    for (File f : filesToRead) {
      pipesSuppliers.add(() -> config.getPipeSupplier().get(new SizedInputStream(new FileInputStream(f), f.length()), f));
    }
    
    return new SyncToAsyncPipe<>(pipesSuppliers, config.getThreadNum());
  }
}
