package org.pipecraft.pipes.sync.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.multi.LocalMultiFileReaderConfig;
import org.pipecraft.pipes.utils.multi.MultiFileReaderUtils;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe reading multiple local binary files.
 * Supports file filtering and ordering, as well as automatic sharding of the data.
 * 
 * @param <T> The output item data type
 *
 * @author Eyal Schneider
 */
public class MultiFileReaderPipe<T> extends CompoundPipe<T> {
  private final LocalMultiFileReaderConfig<T> config;

  /**
   * Constructor
   * 
   * @param config The multi reader config
   */
  public MultiFileReaderPipe(LocalMultiFileReaderConfig<T> config) {
    this.config = config;
  }
  
  @Override
  protected Pipe<T> createPipeline() throws PipeException, InterruptedException {
    try {
      // Determine all files to read
      Collection<File> filesToRead = MultiFileReaderUtils.getAllLocalFilesToRead(config);
         
      // Sort
      if (config.getFileOrder() != null) {
        List<File> sortedFiles = new ArrayList<>(filesToRead);
        Collections.sort(sortedFiles, config.getFileOrder());
        filesToRead = sortedFiles;
      }
      
      // Create pipe suppliers
      List<PipeSupplier<T>> pipesSuppliers = new ArrayList<>();
      for (File f : filesToRead) {
        pipesSuppliers.add(() -> config.getPipeSupplier().get(new SizedInputStream(new FileInputStream(f), f.length()), f));
      }
      
      // Concat
      return new ConcatPipe<>(pipesSuppliers);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
