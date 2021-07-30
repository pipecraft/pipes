package org.pipecraft.pipes.terminal;

import java.io.File;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.infra.math.ArithmeticUtils;
import org.pipecraft.pipes.serialization.EncoderFactory;

/**
 * An async sharder pipe, writing items to files based on the hash value of individual items.
 * The callers can't assume anything regarding the hash function used internally.
 * 
 * @param <T> The items' data type
 *
 * @author Eyal Schneider
 */
public class AsyncSharderByHashPipe <T> extends AsyncSharderPipe<T>{

  /**
   * Constructor 
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardCount The required number of shards (files) to produce. Their names will be numbers in the rage 0 to shardCount - 1.
   * @param folder The folder where to place all shards. Must exist.
   * @param writeOptions Specify how the shard files should be written
   */
  public AsyncSharderByHashPipe(AsyncPipe<T> input, EncoderFactory<? super T> encoderFactory, int shardCount, File folder, FileWriteOptions writeOptions) {
    super(input, encoderFactory, v -> getShardFor(v, shardCount), folder, writeOptions);
  }
  
  /**
   * Constructor 
   * 
   * Uses default file write options.
   * @param input The input pipe
   * @param encoderFactory The encoder to use for writing items into the different shards
   * @param shardCount The required number of shards (files) to produce. Their names will be numbers in the rage 0 to shardCount - 1.
   * @param folder The folder where to place all shards. Must exist.
   */
  public AsyncSharderByHashPipe(AsyncPipe<T> input, EncoderFactory<? super T> encoderFactory, int shardCount, File folder) {
    super(input, encoderFactory, v -> getShardFor(v, shardCount), folder);
  }
  
  private static <V> String getShardFor(V item, int shardCount) {
    return String.valueOf(ArithmeticUtils.getShardByHash(item, shardCount));
  }
}
