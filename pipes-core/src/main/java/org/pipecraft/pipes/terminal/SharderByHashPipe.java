package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.infra.math.ArithmeticUtils;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.File;
import java.util.function.Function;

/**
 * A terminal pipe that splits the contents of the input pipe into multiple files, according to a hash on a some feature of the item.
 * The original order is preserved in each shard.
 * Note that this implementation keeps all shard files open at the same time, so make sure the system can handle this number of open files.
 * 
 * @param <T> The input items' data type
 *
 * @author Eyal Schneider
 */
public class SharderByHashPipe<T> extends SharderByItemPipe<T>{

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param featureSelectorFunction Given an item, selects some feature from it to be hashed and used for shard selection. Must not return null for any non null input!
   * @param fileNameFunction Given a shard id, returns the file corresponding file name
   * @param shardCount The required number of shards.
   * @param folder The folder where to place all shards. Must exist. The files will be named according to fileNameFunction.
   * @param writeOptions Specify how the shard files should be written
   */
  public SharderByHashPipe(
      Pipe<T> input, EncoderFactory<T> encoderFactory, FailableFunction<? super T, ?, PipeException> featureSelectorFunction,
      Function<Integer, String> fileNameFunction, int shardCount, File folder, FileWriteOptions writeOptions) {
    super(input, encoderFactory, transformFunction(featureSelectorFunction, fileNameFunction, shardCount), folder, writeOptions);
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param featureSelectorFunction Given an item, selects some feature from it to be hashed and used for shard selection. Must not return null for any non null input!
   * @param shardCount The required number of shards.
   * @param folder The folder where to place all shards. Must exist. The files will be named "0","1","2"...shardCount-1
   * @param writeOptions Specify how the shard files should be written
   */
  public SharderByHashPipe(Pipe<T> input, EncoderFactory<T> encoderFactory, FailableFunction<? super T, ?, PipeException> featureSelectorFunction, int shardCount, File folder, FileWriteOptions writeOptions) {
    this(input, encoderFactory, featureSelectorFunction, String::valueOf, shardCount, folder, writeOptions);
  }

  private static <T> FailableFunction<? super T, String, PipeException> transformFunction(FailableFunction<T, ?, PipeException> selectorFunction, Function<Integer, String> fileNameFunction, int shardCount) {
    return v -> fileNameFunction.apply(
        ArithmeticUtils.getShardByHash(selectorFunction.apply(v), shardCount));
  }
}
