package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * A terminal async pipe writing all contents from an input pipe into a given {@link java.util.Map}.
 * 
 * Thread safety:
 * It's the responsibility of the caller to use a thread safe map object!
 *
 * @param <T> The type of items in the input pipe
 * @param <K> The key type in the output map. Must be suitable as a key map (i.e. equal items must have the same hashcode).
 * @param <V> The value type in the output map
 *
 * @author Eyal Schneider
 */
public class AsyncMapWriterPipe<T, K, V> extends TerminalPipe {
  private final AsyncPipe<T> input;
  private final Map<K, V> outputMap;
  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private PipeException exception;

  /**
   * Constructor
   *
   * @param inputPipe The input pipe to consume
   * @param keyExtractor The logic for extracting the map key from an input item. In case that two different items produce the same key, an arbitrary item will take precedence and override the results on the other.
   * @param valueExtractor  The logic for extracting the map value from an input item
   * @param outputMap The map to populate
   */
  public AsyncMapWriterPipe(AsyncPipe<T> inputPipe, FailableFunction<T, K, ValidationPipeException> keyExtractor, FailableFunction<T, V, ValidationPipeException> valueExtractor, Map<K, V> outputMap) {
    this.input = inputPipe;
    this.outputMap = outputMap;

    input.setListener(new AsyncPipeListener<>() {
      @Override
      public void next(T item) throws PipeException {
        K key = keyExtractor.apply(item);
        V value = valueExtractor.apply(item);
        outputMap.put(key, value);
      }

      @Override
      public void done() {
        terminationLatch.countDown();
      }

      @Override
      public void error(PipeException e) {
        exception = e;
        terminationLatch.countDown();
      }
    });
  }

  /**
   * Constructor
   *
   * Collects items into a {@link java.util.concurrent.ConcurrentHashMap} created internally.
   * @param inputPipe The input pipe to consume
   * @param keyExtractor The logic for extracting the map key from an input item. In case that two different items produce the same key, an arbitrary item will take precedence and override the results on the other.
   * @param valueExtractor  The logic for extracting the map value from an input item
   */
  public AsyncMapWriterPipe(AsyncPipe<T> inputPipe, FailableFunction<T, K, ValidationPipeException> keyExtractor, FailableFunction<T, V, ValidationPipeException> valueExtractor) {
    this(inputPipe, keyExtractor, valueExtractor, new ConcurrentHashMap<>());
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    terminationLatch.await();
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * @return The output map
   */
  public Map<K, V> getItems() {
    return outputMap;
  }
}
