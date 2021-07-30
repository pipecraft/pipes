package org.pipecraft.pipes.sync.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe that reads lines from a given reader.
 * Note that since this class is based on a textual input stream only, it can't support progress tracking. Subclasses are encouraged to use the stream origin
 * for supporting progress tracking.
 * 
 * @author Eyal Schneider
 */
public class ReaderPipe implements Pipe<String> {
  private final BufferedReader reader;
  private String next;
  private volatile boolean done;

  /**
   * Constructor
   * 
   * @param r The reader to read from. Wrapped by a BufferedReader by this class.
   */
  public ReaderPipe(Reader r) {
    this.reader = new BufferedReader(r);    
  }

  /**
   * Constructor
   * 
   * @param r The reader to read from. Wrapped by a BufferedReader by this class.
   * @param bufferSize The buffer size of the wrapping BufferedReader, in bytes
   */
  public ReaderPipe(Reader r, int bufferSize) {
    this.reader = new BufferedReader(r, bufferSize);
  }

  @Override
  public void start() throws PipeException {
    prepareNext();
  }
  
  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public String next() throws PipeException {
    String toReturn = next;
    prepareNext();
    if (next == null) {
      done = true;
    }
    return toReturn;
  }

  @Override
  public String peek() {
    return next;
  }

  private void prepareNext() throws PipeException {
    try {
      next = reader.readLine();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public float getProgress() {
    // Basic implementation, due to missing information about the reader origin. Subclasses should have better implementations.
    if (done) {
      return 1.0f;
    }
    return 0.0f;
  }
}
