package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A base class for item decoders whose pre-processing on the input streams consists only
 * on buffering and decompression, providing the transformed input stream for the decode method to work on.
 * Handles input stream closure.
 *
 * @param <T> The data type of decoded items
 *
 * @author Eyal Schneider
 */
public abstract class AbstractInputStreamItemDecoder<T> implements ItemDecoder<T> {
  protected final BufferedInputStream is;

  /**
   * Constructor
   * @param is The input stream to decode items from
   * @param readOptions The read options defining how to handle the input stream
   * @throws IOException In case of IO error when preparing to read from the input stream
   */
  public AbstractInputStreamItemDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    this.is = FileUtils.getInputStream(is, readOptions);
  }

  @Override
  public void close() throws IOException {
    is.close();
  }
}
