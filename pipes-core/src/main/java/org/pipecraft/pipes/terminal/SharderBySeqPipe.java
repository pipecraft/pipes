package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.inter.IntermediateSharderBySeqPipe;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.File;
import java.util.Map;

/**
 * A terminal pipe that splits the contents of the input pipe according to some criteria which breaks the input pipe into disjoint contiguous sequences.
 * Unlike other sharder pipes, this implementation assumes that input items are already grouped by target shard, therefore it can work file by file, avoiding the
 * need to maintain many open files at the same time.
 * 
 * Note that if a sequence corresponds to an already processed shard, the shard's file will be overwritten.
 * 
 * @param <T> The input items' data type
 *
 * @author Eyal Schneider
 */
public class SharderBySeqPipe<T> extends CompoundTerminalPipe {
  private final Pipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final FailableFunction<? super T, String, PipeException> selector;
  private final File folder;
  private final FileWriteOptions fileWriteOptions;
  private volatile Map<String, Integer> shardStats;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   * @param fileWriteOptions Define how files should be written
   */
  public SharderBySeqPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder, FileWriteOptions fileWriteOptions) {
    this.input = input;
    this.encoderFactory = encoderFactory;
    this.selector = shardSelectorFunction;
    this.folder = folder;
    this.fileWriteOptions = fileWriteOptions;
  }

  /**
   * Constructor
   *
   * Uses default file write options
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. May be stateful. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   */
  public SharderBySeqPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder) {
    this(input, encoderFactory, shardSelectorFunction, folder, new FileWriteOptions());
  }

  @Override
  protected TerminalPipe createPipeline() throws PipeException, InterruptedException {
    IntermediateSharderBySeqPipe<T> writerP = new IntermediateSharderBySeqPipe<>(input, encoderFactory, selector, folder, fileWriteOptions);
    return new ConsumerPipe<>(writerP, () -> shardStats = writerP.getShardSizes());
  }

  /**
   * @return The counts of items written to each shard. Call this method only after start() has been called and completed successfully. 
   */
  public Map<String, Integer> getShardSizes() {
    return shardStats;
  }
}
