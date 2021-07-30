package org.pipecraft.infra.storage.google_cs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Takes part in sliced transfer, making sure that:
 * 1) The local file writer is created lazilly, only when we start downloading a slice belonging to the file
 * 2) The local file's writer is closed immediately after writing the last slice of the file
 * 
 * @author Eyal Schneider
 */
public class SlicedTransferFileHandler {
  private final File file;
  private final int fileSliceCount;
  private final AtomicInteger completedSliceCounter = new AtomicInteger();
  private final Object channelCreationLock = new Object();
  private volatile FileChannel fileChannel;
  
  /**
   * Constructor
   *  
   * @param file The local file we are writing to
   * @param fileSliceCount The total number of slices to be written to the file
   */
  public SlicedTransferFileHandler(File file, int fileSliceCount) {
    this.file = file;
    this.fileSliceCount = fileSliceCount;
  }
  
  /**
   * @return The writer to be used for writing to the local file. Created lazilly.
   * @throws IOException In case the file can't be created
   */
  public FileChannel getWriter() throws IOException {
    FileChannel channel = fileChannel;
    if (channel == null) {
      synchronized(channelCreationLock) {
        if (fileChannel == null) {
          fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } 
        channel = fileChannel;
      }
    }    
    return channel;
  }
  
  /**
   * Call this method each time a slice download for this file is complete.
   * Once the method detects all slices are complete, the file is closed
   * @throws IOException In case the file can't be closed
   */
  public void doneSliceProcessing() throws IOException {
    if (completedSliceCounter.incrementAndGet() == fileSliceCount) {
      fileChannel.close();
    }
  }
}
