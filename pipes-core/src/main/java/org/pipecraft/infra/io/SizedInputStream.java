package org.pipecraft.infra.io;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * A simple {@link InputStream} wrapper, that adds awareness to the stream size
 *
 * @author Eyal Schneider
 */
public class SizedInputStream extends FilterInputStream {

  private final Long size;

  /**
   * Package protected constructor
   *
   * @param in The wrapped input stream
   * @param size The stream size, in bytes. Null means unknown.
   */
  public SizedInputStream(InputStream in, Long size) {
    super(in);
    this.size = size;
  }

  /**
   * @return The stream size, in bytes. Null means unknown.
   */
  public Long getSize() {
    return size;
  }
}

