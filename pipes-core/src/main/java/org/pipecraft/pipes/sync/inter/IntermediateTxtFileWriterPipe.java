package org.pipecraft.pipes.sync.inter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import java.nio.charset.StandardCharsets;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;


/**
 * A pipe write to a text file any item passed through it.
 * The write callback is never invoked on null values which indicate end of input pipe.
 * Pipe output is same as pipe input.
 * In order to write the object as a string, a string mapper must be provided.
 *
 * @author Shai Barad
 */
public class IntermediateTxtFileWriterPipe<T> extends DelegatePipe<T> {

  private final FailableFunction<T, String, PipeException> stringMapper;
  private final File output;
  private final Charset charset;
  private final FileWriteOptions writeOptions;
  private BufferedWriter bw;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param charset the file charset
   * @param options the file options
   * @param stringMapper in order to write the object as a string, a string mapper must be provided
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, Charset charset, FileWriteOptions options, FailableFunction<T, String, PipeException> stringMapper) {
    super(input);
    this.output = output;
    this.charset = charset;
    this.writeOptions = options;
    this.stringMapper = stringMapper;
  }

  /**
   * Constructor
   *
   * Uses the toString as the mapper from the object to String
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param charset the file charset
   * @param options the file options
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, Charset charset, FileWriteOptions options) {
    this(input, output, charset, options, Object::toString);
  }

  /**
   * Constructor
   *
   * Uses default file write options.
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param charset the file charset
   * @param stringMapper in order to write the object as a string, a string mapper must be provided. It should never return null values
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, Charset charset, FailableFunction<T, String, PipeException> stringMapper) {
    this(input, output, charset, new FileWriteOptions(), stringMapper);
  }

  /**
   * Constructor
   *
   * Uses default file write options and UTF8.
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param stringMapper in order to write the object as a string, a string mapper must be provided. It should never return null values
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, FailableFunction<T, String, PipeException> stringMapper) {
    this(input, output, StandardCharsets.UTF_8, stringMapper);
  }

  /**
   * Constructor
   *
   * Uses default toString() as the string mapper, and UTF8
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param options the file options
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, FileWriteOptions options) {
    this(input, output, StandardCharsets.UTF_8, options);
  }

  /**
   * Constructor
   *
   * Uses UTF8
   * @param input The input pipe
   * @param output The text file to write to. If exists, the file is being overwritten.
   * @param options The file writing options
   * @param stringMapper in order to write the object as a string, a string mapper must be provided. It should never return null values
   */
  public IntermediateTxtFileWriterPipe(Pipe<T> input, File output, FileWriteOptions options, FailableFunction<T, String, PipeException> stringMapper) {
    this(input, output, StandardCharsets.UTF_8, options, stringMapper);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (bw != null) {
      bw.close();
    }
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    try {
      T next = getOriginPipe().next();

      if (next != null) {
        bw.write(stringMapper.apply(next));
        bw.newLine();
      }
      return next;
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      this.bw = FileUtils.getWriter(output, charset, writeOptions);
      super.start();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
