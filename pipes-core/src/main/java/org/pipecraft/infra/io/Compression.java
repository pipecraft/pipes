package org.pipecraft.infra.io;

import java.io.File;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;

/**
 * Supported compression types
 *
 * @author Tomer Zeltzer, Eyal Schneider
 */
public enum Compression {
  NONE(null, 0),
  GZIP(".gz", -1), // No compression levels
  ZSTD(".zst", 3), // 1(fastest) to 22
  LZ4(".lz4", 0);  // Either 1-17 (for "high compressor" levels) or 0 for the fast compressor implementation.

  private final String fileExtension;
  private final int defaultCompressionLevel;

  Compression(String fileExtension, int defaultCompressionLevel) {
    this.fileExtension = fileExtension;
    this.defaultCompressionLevel = defaultCompressionLevel;
  }

  /**
   * @return The standard file extension representing this compression method
   */
  public String getFileExtension() {
    return fileExtension;
  }

  /**
   * @return The default compression level of this compression method
   */
  public int getDefaultCompressionLevel() {
    return defaultCompressionLevel;
  }
  
  /**
   * Detects compression based on file extension
   *
   * @param filename File name to detect compression
   * @return The detected compression, or None if none of the supported compressions matched
   */
  public static Compression detect(String filename) {
    return Arrays.stream(values())
        .filter(compression -> StringUtils.endsWith(filename, compression.fileExtension))
        .findAny()
        .orElse(NONE);
  }

  /**
   * Detects compression based on file extension
   *
   * @param file File to detect compression for
   * @return The detected compression, or None if none of the supported compressions matched
   */
  public static Compression detect(File file) {
    return detect(file.getName());
  }

  /**
   * @param filename A filename, to be appended with the proper extension
   * @return The filename, suffixed with the extension string corresponding to this compression. For type NONE, no extension is added.
   */
  public String withExtension(String filename) {
    return filename + (fileExtension == null ? "" : fileExtension);
  }
}
