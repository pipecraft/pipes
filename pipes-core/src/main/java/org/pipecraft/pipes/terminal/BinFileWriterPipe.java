package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.inter.IntermediateBinFileWriterPipe;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.File;

/**
 * A terminal pipe writing items to a local file in a binary format. Supports gz compression.
 * Calling start() blocks until all data from the input pipe is written to the file.
 * 
 * @author Eyal Schneider
 */
public class BinFileWriterPipe<T> extends CompoundTerminalPipe {
  private final FileWriteOptions writeOptions;
  private final Pipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final File outputFile;
  
  /**
   * Constructor
   * 
   * @param input The input pipe to read items from
   * @param outputFile The output file
   * @param options The file writing options
   * @param encoderFactory The encoder factory to use for converting items to binary form
   */
  public BinFileWriterPipe(Pipe<T> input, File outputFile, FileWriteOptions options, EncoderFactory<? super T> encoderFactory) {
    if (options.isTemp()) {
      outputFile.deleteOnExit();
    }
    this.writeOptions = options;
    this.input = input;
    this.encoderFactory = encoderFactory;
    this.outputFile = outputFile;
  }

  /**
   * Constructor
   * 
   * Uses default file write settings (see @{FileWriteOptions})
   * @param input The input pipe to read items from
   * @param outputFile The output file
   * @param encoderFactory The encoder factory to use for converting items to binary form
   */
  public BinFileWriterPipe(Pipe<T> input, File outputFile, EncoderFactory<? super T> encoderFactory) {
    this(input, outputFile, new FileWriteOptions(), encoderFactory);
  }

  @Override
  protected TerminalPipe createPipeline() throws PipeException, InterruptedException {
    Pipe<T> inter = new IntermediateBinFileWriterPipe<>(input, outputFile, writeOptions, encoderFactory);
    return new ConsumerPipe<>(inter);
  }
}
