package org.pipecraft.infra.storage.google_cs;

import com.google.cloud.storage.StorageException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A simple {@link InputStream} wrapper, that converts {@link StorageException}s to IOExceptions, as expected.
 * 
 * @author Eyal Schneider
 */
public class GSInputStream extends FilterInputStream {

  /**
   * Package protected constructor
   * 
   * @param in The wrapped input stream. Any {@link StorageException} originated from this stream are converted into legitimate {@link IOException}.
   */
  GSInputStream(InputStream in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    try {
      return super.read();
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    try {
      return super.read(b, off, len);
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try {
      return super.skip(n);
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int available() throws IOException {
    try {
      return super.available();
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

  @Override
  public synchronized void reset() throws IOException {
    try {
      super.reset();
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }
}