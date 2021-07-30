package org.pipecraft.infra.io;

/**
 * Encapsulates file reading settings, with defaults.
 * 
 * @author Eyal Schneider
 */
public class FileReadOptions {
  private int bufferSize = 8192;
  private Compression compression = Compression.NONE;

  /**
   * Sets the compression of the file
   *
   * @param compression the compression of the file
   * @return the file read options
   */
  public FileReadOptions setCompression(Compression compression) {
    this.compression = compression;
    return this;
  }

  /**
   * Sets the compression of the file by its extension
   *
   * @param filename The file's name
   * @return the file read options
   */
  public FileReadOptions detectCompression(String filename) {
    this.compression = Compression.detect(filename);
    return this;
  }

  /**
   * Sets the buffer size (default = 8192)
   * @param bufferSize The read buffer size, in bytes
   * @return this instance
   */
  public FileReadOptions buffer(int bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  /**
   * @return The read buffer size, in bytes
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * @return The compression type of this file
   */
  public Compression getCompression() {
    return compression;
  }
}
