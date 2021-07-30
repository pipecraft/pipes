package org.pipecraft.infra.io;

/**
 * Encapsulates file writing settings, with defaults.
 * 
 * @author Eyal Schneider
 */
public class FileWriteOptions {
  private boolean isAppend = false;
  private int bufferSize = 8192;
  private Compression compression = Compression.NONE;
  private int compressionLevel;
  private boolean isTemp = false;

  /**
   * Sets the append flag (by default it's false)
   * @return this instance
   */
  public FileWriteOptions append() {
    this.isAppend = true;
    return this;
  }

  /**
   * @return The compression
   */
  public Compression getCompression() {
    return compression;
  }

  /**
   * @return The compression level to use, if compression is set. Specific to the selected compression algorithm. 
   */
  public int getCompressionLevel() {
    return compressionLevel;
  }

  /**
   * Sets the compression of the file
   *
   * @param compression the compression of the file
   * @return the file write options
   */
  public FileWriteOptions setCompression(Compression compression) {
    setCompression(compression, compression.getDefaultCompressionLevel());
    return this;
  }

  /**
   * Sets the compression of the file
   *
   * @param compression the compression of the file
   * @return the file write options
   */
  public FileWriteOptions setCompression(Compression compression, int compressionLevel) {
    this.compression = compression;
    this.compressionLevel = compressionLevel;
    return this;
  }

  /**
   * Sets the buffer size (default = 8192)
   * @param bufferSize The write buffer size, in bytes
   * @return this instance
   */
  public FileWriteOptions buffer(int bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  /**
   * Sets the file to be temporary, meaning that it will be deleted once the JVM exits gracefully (by default it's false)
   * @return this instance
   */
  public FileWriteOptions temp() {
    this.isTemp = true;
    return this;
  }

  /**
   * if true, sets compression the flag on file write options.
   *
   * @param temp True for temporary file, false for otherwise. Temporary files are deleted at graceful JVM shutdown.
   * @return the file write options
   */
  public FileWriteOptions temp(boolean temp) {
   this.isTemp = temp;
   return this;
  }

  /**
   * @return True for appending to an existing file, false for overriding
   */
  public boolean isAppend() {
    return isAppend;
  }

  /**
   * @return The write buffer size, in bytes
   */
  public int getBufferSize() {
    return bufferSize;
  }
  
  /**
   * @return True for temporary file, false for otherwise. Temporary files are deleted at graceful JVM shutdown.
   */
  public boolean isTemp() {
    return isTemp;
  }
}
