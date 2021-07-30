package org.pipecraft.infra.storage.google_cs;

import com.google.cloud.storage.StorageException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A simple {@link OutputStream} wrapper, that converts {@link StorageException}s to IOExceptions, as expected.
 * 
 * @author Eyal Schneider
 */
public class GSOutputStream extends FilterOutputStream {

  /**
   * Package protected constructor
   * 
   * @param os The wrapped output stream. Any {@link StorageException} originated from this stream are converted into legitimate {@link IOException}.
   */
  GSOutputStream(OutputStream os) {
    super(os);
  }

  @Override
  public void write(int b) throws IOException {
    try {
      super.write(b);
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    try {
      super.flush();
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }
}