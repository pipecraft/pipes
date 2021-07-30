/**
 * 
 */
package org.pipecraft.infra.storage.google_cs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import java.io.Closeable;
import java.io.IOException;
import org.pipecraft.infra.io.FileUtils;

/**
 * Contains the details of a specific slice to be transferred, including the read/write handles.
 * The read channel is created lazily when requested.
 * 
 * @author Oren Peer, Eyal Schneider
 */
public class SliceTransferJobDetails implements Closeable {
  private final SlicedTransferFileHandler targetFileHandler;
  private final long pos;
  private final long length;
  private final Blob readBlob;
  private final int chunkSize;
  private ReadChannel readChannel;
  
  /**
   * 
   * @param readBlob The blob to read from
   * @param chunkSize The download chunk size to use
   * @param targetFileHandler The handler for the file to write to. The same handler should be used in different jobs that refer to different slices of the same file.
   * @param start The file position at which the slice reading should begin. Non-negative.
   * @param length The size (in bytes) the reader should read
   */
  public SliceTransferJobDetails(Blob readBlob, int chunkSize, SlicedTransferFileHandler targetFileHandler, long start, long length) {
    this.readBlob = readBlob;
    this.chunkSize = chunkSize;
    this.targetFileHandler = targetFileHandler;
    this.pos = start;
    this.length = length;
  }

  /**
   * @return the channel for reading data from
   */
  public ReadChannel getReadChannel() {
    if (readChannel == null) { // Create the channel lazily
      readChannel = readBlob.reader();
      readChannel.setChunkSize(chunkSize);
    }
    return readChannel;
  }

  /**
   * @return The handler for the file to write to. The same handler should be used in different jobs that refer to different slices of the same file.
   */
  public SlicedTransferFileHandler getTargetFileHandler() {
    return targetFileHandler;
  }

  /**
   * @return The file position at which the slice reader is begin;
   */
  public long getPosition() {
    return pos;
  }

  /**
   * @return The size (in bytes) the reader should read. 
   */
  public long getLength() {
    return length;
  }

  @Override
  public void close() throws IOException {
    FileUtils.close(readChannel, targetFileHandler.getWriter());
  }
}
