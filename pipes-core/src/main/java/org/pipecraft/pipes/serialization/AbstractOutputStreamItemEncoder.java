package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A base class for item encoders whose pre-processing on the output streams consists only
 * on buffering and compression, providing the transformed output stream for the encode method to work on.
 * Handles output stream closure.
 *
 * @param <T> The data type of the items to encode
 *
 * @author Eyal Schneider
 */
public abstract class AbstractOutputStreamItemEncoder<T> implements ItemEncoder<T> {
  protected final BufferedOutputStream os;

  /** Constructor
   * @param os The output stream to encode items into
   * @param writeOptions The write options defining how to handle the output stream
   * @throws IOException In case of IO error when preparing to write to the output stream
   */
  public AbstractOutputStreamItemEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    this.os = FileUtils.getOutputStream(os, writeOptions);
  }

  @Override
  public void close() throws IOException {
    os.close();
  }
}
