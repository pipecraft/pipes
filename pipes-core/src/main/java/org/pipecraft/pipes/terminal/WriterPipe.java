package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * A terminal pipe writing textual items to a given {@link Writer}, one item per line.
 * Calling start() blocks until all data from the input pipe is written to the writer.
 *
 * @author Eyal Schneider
 */
public class WriterPipe extends TerminalPipe {

  private final BufferedWriter bw;
  private final Pipe<String> input;

  /**
   * Constructor
   *
   * @param input The input pipe to read items from
   * @param w The writer to write to
   */
  public WriterPipe(Pipe<String> input, Writer w) {
    this.bw = new BufferedWriter(w);
    this.input = input;
  }

  /**
   * Constructor
   *
   * @param input The input pipe to read items from
   * @param w The writer to write to
   * @param bufferSize the write buffer to use
   */
  public WriterPipe(Pipe<String> input, Writer w, int bufferSize) {
    this.bw = new BufferedWriter(w, bufferSize);
    this.input = input;
  }

  @Override
  public void close() throws IOException {
    bw.close();
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    String item;
    try {
      while ((item = input.next()) != null) {
        bw.write(item);
        bw.newLine();
      }
      bw.flush();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
