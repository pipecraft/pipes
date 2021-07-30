package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.source.TxtFileReaderPipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A terminal pipe writing text lines from the input pipe into a file. Supports compression.
 * @see TxtFileReaderPipe
 *
 * @author Eyal Schneider
 */
public class TxtFileWriterPipe extends TerminalPipe {
  private final Pipe<String> input;
  private final FileWriteOptions writeOptions;
  private final Charset charset;
  private final File file;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param f The text file to write to, one item per line
   * @param charset The charset to use
   * @param options The file writing options
   */
  public TxtFileWriterPipe(Pipe<String> input, File f, Charset charset, FileWriteOptions options) {
    this.input = input;
    this.file = f;
    this.charset = charset;
    this.writeOptions = options;
  }

  /**
   * Constructor
   *
   * Uses default file write options.
   * @param input The input pipe
   * @param f The text file to write to. If exists, the file is being overwritten.
   * @param charset The charset to use
   */
  public TxtFileWriterPipe(Pipe<String> input, File f, Charset charset) {
    this(input, f, charset, new FileWriteOptions());
  }

  /**
   * Constructor
   *
   * Uses default file write options and UTF8 charset encoding.
   * @param input The input pipe
   * @param f The text file to write to. If exists, the file is being overwritten.
   */
  public TxtFileWriterPipe(Pipe<String> input, File f) {
    this(input, f, StandardCharsets.UTF_8, new FileWriteOptions());
  }

  /**
   * Constructor
   *
   * Uses UTF8 charset encoding.
   * @param input The input pipe
   * @param f The text file to write to. If exists, the file is being overwritten.
   * @param options The file writing options
   */
  public TxtFileWriterPipe(Pipe<String> input, File f, FileWriteOptions options) {
    this(input, f, StandardCharsets.UTF_8, options);
  }


  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    String item;
    try (BufferedWriter bw = FileUtils.getWriter(file, charset, writeOptions)){
      while ((item = input.next()) != null) {
        bw.write(item);
        bw.newLine();
      }
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

}
