package org.pipecraft.pipes.sync.inter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.serialization.ItemEncoder;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.terminal.BinFileWriterPipe;

/**
 * The intermediate version of {@link BinFileWriterPipe}.
 * Writes items to a local file in a binary format. Supports compression.
 * 
 * @author Eyal Schneider
 */
public class IntermediateBinFileWriterPipe<T> extends DelegatePipe<T> {
  private final FileWriteOptions writeOptions;
  private final Pipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final File outputFile;
  private ItemEncoder<? super T> encoder;
  
  /**
   * Constructor
   * 
   * @param input The input pipe to read items from
   * @param outputFile The output file
   * @param options The file writing options
   * @param encoderFactory The encoder factory to use for converting items to binary form
   */
  public IntermediateBinFileWriterPipe(Pipe<T> input, File outputFile, FileWriteOptions options, EncoderFactory<? super T> encoderFactory) {
    super(input);
    if (options.isTemp()) {
      outputFile.deleteOnExit();
    }
    this.outputFile = outputFile;
    this.writeOptions = options;
    this.input = input;
    this.encoderFactory = encoderFactory;
  }

  /**
   * Constructor
   * 
   * Uses default file write settings (see @{FileWriteOptions})
   * @param input The input pipe to read items from
   * @param outputFile The output file
   * @param encoderFactory The encoder factory to use for converting items to binary form
   */
  public IntermediateBinFileWriterPipe(Pipe<T> input, File outputFile, EncoderFactory<? super T> encoderFactory) {
    this(input, outputFile, new FileWriteOptions(), encoderFactory);
  }

  @Override
  public void close() throws IOException {
    input.close();
    if (encoder != null) {
      encoder.close();
    }
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    super.start();
    try {
      OutputStream os = new FileOutputStream(outputFile, writeOptions.isAppend());
      encoder = encoderFactory.newEncoder(os, writeOptions);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
  
  @Override
  public T next() throws PipeException, InterruptedException {
    try {
      T next = super.next();
      if (next != null) {
        encoder.encode(next);
      }
      return next;
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
