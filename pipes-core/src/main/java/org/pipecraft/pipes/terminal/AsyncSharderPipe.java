package org.pipecraft.pipes.terminal;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.serialization.ItemEncoder;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.InternalPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A terminal pipe that receives an async pipe as input, and splits the contents of the input pipe into multiple files
 * according to some sharding criteria based on individual items.
 * The async input allows high throughput through parallel writes to different files. The writing is done using the threads provided by the input pipe.
 * The implementation allows calling close() by any thread after start() has been invoked.
 * 
 * Note that this implementation keeps all shard files open at the same time, so make sure the system can handle this number of open files.
 * 
 * @param <T> The input items' data type
 *
 * @author Eyal Schneider
 */
public class AsyncSharderPipe<T> extends TerminalPipe {
  private final AsyncPipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final Function<? super T, String> selector;
  private final File folder;
  private final FileWriteOptions writeOptions;
  private final Map<String, ShardWriter> shardWriters = new ConcurrentHashMap<>();
  private volatile Map<String, Integer> counts;
  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private volatile PipeException error;

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   * @param writeOptions Specify how the shard files should be written
   */
  public AsyncSharderPipe(AsyncPipe<T> input, EncoderFactory<? super T> encoderFactory, Function<? super T, String> shardSelectorFunction, File folder, FileWriteOptions writeOptions) {
    this.input = input;
    this.encoderFactory = encoderFactory;
    this.selector = shardSelectorFunction;
    this.folder = folder;
    this.writeOptions = writeOptions;
  }

  /**
   * Constructor
   * 
   * Uses default file write options
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   */
  public AsyncSharderPipe(AsyncPipe<T> input, EncoderFactory<? super T> encoderFactory, Function<? super T, String> shardSelectorFunction, File folder) {
    this(input, encoderFactory, shardSelectorFunction, folder, new FileWriteOptions());
  }

  @Override
  public void close() throws IOException {
    input.close();
    terminationLatch.countDown(); // Release caller thread running the start() method
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.setListener(new AsyncPipeListener<>() {

      @Override
      public void next(T item) throws PipeException, InterruptedException {
        String shardId = selector.apply(item);
        try {
          shardWriters.computeIfAbsent(shardId, k -> {
            try {
              return new ShardWriter(k);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }).write(item);
        } catch (UncheckedIOException | IOException e) {
          throw new IOPipeException(e);
        }
      }

      @Override
      public void done() throws InterruptedException {
        terminationLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        error = e;
        terminationLatch.countDown();
      }
    });
    
    input.start();
    terminationLatch.await(); // Block until all items are processed, an error occurs or close() is called
    
    // Flush and close all writers data
    try {
      FileUtils.close(shardWriters.values());
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
    
    // Handle error, if any
    if (error != null) {
      if (error instanceof InternalPipeException) {
        throw ((InternalPipeException) error).getRuntimeException(); // Transforming the wrapper to the actual runtime exception it should be
      }
      throw error;
    }
    
    // Publish stats
    counts = Collections.unmodifiableMap(shardWriters.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> v.getValue().getItemsWrittenCount())));
  }
  
  /**
   * @return The counts of items written to each shard. Call this method only after start() has been called and completed successfully. 
   */
  public Map<String, Integer> getShardSizes() {
    return counts;
  }
  
  @Override
  public float getProgress() {
    return input.getProgress();
  }

  // Writes to a specific shard file. Thread safe.
  private class ShardWriter implements Closeable {
    private final ItemEncoder<? super T> encoder;
    private int itemsWrittenCount;
    
    public ShardWriter(String shardId) throws IOException {
      OutputStream os = new FileOutputStream(new File(folder, shardId), writeOptions.isAppend());
      encoder = encoderFactory.newEncoder(os, writeOptions);
    }
    
    public synchronized void write(T item) throws IOException {
      encoder.encode(item);
      itemsWrittenCount++;
    }

    @Override
    public synchronized void close() throws IOException {
      encoder.close();
    }
    
    public synchronized int getItemsWrittenCount() {
      return itemsWrittenCount;
    }
  }
}
