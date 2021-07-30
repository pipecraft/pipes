package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.serialization.ItemEncoder;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * An intermediate version of {@link org.pipecraft.pipes.terminal.SharderBySeqPipe}
 *
 * @param <T> The items' data type
 *
 * @author Eyal Schneider
 */
public class IntermediateSharderBySeqPipe<T> implements Pipe<T> {
  private final Pipe<T> input;
  private final EncoderFactory<? super T> encoderFactory;
  private final FailableFunction<? super T, String, PipeException> selector;
  private final File folder;
  private final FileWriteOptions fileWriteOptions;
  private Map<String, MutableInt> counts;
  private volatile Map<String, Integer> finalCounts;
  private ItemEncoder<? super T> encoder;
  private T next;
  private String curShardId;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param encoderFactory The encoder factory to use for writing items into the different shards
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. May be stateful. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   * @param fileWriteOptions Define how files should be written
   */
  public IntermediateSharderBySeqPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder, FileWriteOptions fileWriteOptions) {
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
   * @param shardSelectorFunction Given an item, selects the corresponding shard id. Files will use this id as a name. Must not return null for any non null input!
   * @param folder The folder where to place all shards. Must exist.
   */
  public IntermediateSharderBySeqPipe(Pipe<T> input, EncoderFactory<? super T> encoderFactory, FailableFunction<? super T, String, PipeException> shardSelectorFunction, File folder) {
    this(input, encoderFactory, shardSelectorFunction, folder, new FileWriteOptions());
  }
  
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    this.counts = new HashMap<>();
    input.start();

    next = input.next();
    if (next != null) {
      curShardId = selector.apply(next);
      encoder = createEncoder(curShardId);
    }
  }

  private void prepareNext() throws PipeException, InterruptedException {
    next = input.next();

    if (next == null) {
      closeNullSafe(encoder);
      return;
    }

    String shardId = selector.apply(next);
    if (!shardId.equals(curShardId)) { // A shard just ended and a new one starts
      closeNullSafe(encoder);
      encoder = createEncoder(shardId);
      curShardId = shardId;
    }
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    try {
      T toReturn = next;
      if (next != null) {
        encoder.encode(next);
        counts.computeIfAbsent(curShardId, id -> new MutableInt()).increment();
        prepareNext();
      } else {
        finalCounts = counts.entrySet().stream().collect(Collectors.toMap(Entry::getKey, p -> p.getValue().intValue()));
      }
      return toReturn;
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public T peek() {
    return next;
  }

  /**
   * @return The counts of items written to each shard. Call this method only after the pipe terminates.
   */
  public Map<String, Integer> getShardSizes() {
    return finalCounts;
  }
  
  private static void closeNullSafe(Closeable closeable) throws IOPipeException {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }

  private ItemEncoder<? super T> createEncoder(String shardId) throws IOPipeException {
    try {
      File shardFile = new File(folder, shardId);
      OutputStream os = new FileOutputStream(shardFile, fileWriteOptions.isAppend());
      if (fileWriteOptions.isTemp()) {
        shardFile.deleteOnExit();
      }
      return encoderFactory.newEncoder(os, fileWriteOptions);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
}
