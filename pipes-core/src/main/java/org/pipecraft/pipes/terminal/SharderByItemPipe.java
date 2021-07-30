package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.serialization.ItemEncoder;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe that splits the contents of the input pipe into multiple files, according to some sharding criteria based on each item.
 * The original order is preserved in each shard.
 * Note that this implementation keeps all shard files open at the same time, so make sure the system can handle this number of open files.
 * 
 * @param <T> The input items' data type
 *
 * @author Eyal Schneider
 */
public class SharderByItemPipe<T> extends TerminalPipe {
  private final Pipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final FailableFunction<? super T, String, PipeException> selector;
  private final File folder;
  private final FileWriteOptions writeOptions;
  private volatile Map<String, Integer> counts;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   * @param writeOptions Specify how the shard files should be written
   */
  public SharderByItemPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder, FileWriteOptions writeOptions) {
    this.input = input;
    this.encoderFactory = encoderFactory;
    this.selector = shardSelectorFunction;
    this.folder = folder;
    this.writeOptions = writeOptions;
  }

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   */
  public SharderByItemPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder) {
    this(input, encoderFactory, shardSelectorFunction, folder, new FileWriteOptions());
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    HashMap<String, ShardWriter> shardWriters = new HashMap<>(); // Maps shard id to the corresponding buffered output stream. Created lazily.
    try {
      T item;
      while ((item = input.next()) != null) {
        String shardId = selector.apply(item);
        ShardWriter w = shardWriters.get(shardId);
        if (w == null) {
          w = new ShardWriter(shardId);
          shardWriters.put(shardId, w);
        }
        w.write(item);
      }
      counts = Collections.unmodifiableMap(shardWriters.entrySet().stream().collect(Collectors.toMap(
          Entry::getKey, v -> v.getValue().getItemsWrittenCount())));
    } catch (IOException e) {
      throw new IOPipeException(e);
    } finally {
      FileUtils.closeSilently(shardWriters.values());
    }
  }
  
  /**
   * @return The counts of items written to each shard. Call this method only after start() has been called and completed successfully. 
   */
  public Map<String, Integer> getShardSizes() {
    return counts;
  }
  
  private class ShardWriter implements Closeable {
    private final ItemEncoder<? super T> encoder;
    private int itemsWrittenCount;
    
    public ShardWriter(String shardId) throws IOException {
      File shardFile = new File(folder, shardId);
      FileOutputStream os = new FileOutputStream(shardFile, writeOptions.isAppend());
      if (writeOptions.isTemp()) {
        shardFile.deleteOnExit();
      }
      this.encoder = encoderFactory.newEncoder(os, writeOptions);
    }
    
    public void write(T item) throws IOException {
      encoder.encode(item);
      itemsWrittenCount++;
    }

    @Override
    public void close() throws IOException {
      encoder.close();
    }
    
    public int getItemsWrittenCount() {
      return itemsWrittenCount;
    }
  }
}
