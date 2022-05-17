package org.pipecraft.pipes.terminal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.Pipe;

/**
 * A terminal pipe writing all contents from an input pipe into a given {@link java.util.Map}.
 *
 * Thread safety:
 * It's the responsibility of the caller to make sure access to the output map is thread
 * safe. For example, if the map itself is thread safe, or if a single thread creates, runs and queries the results (using getItems()) then
 * there's no concurrency problem.
 *
 * @param <T> The type of items in the input pipe
 * @param <K> The key type in the output map. Must be suitable as a key map (i.e. equal items must have the same hashcode).
 * @param <V> The value type in the output map
 *
 * @author Eyal Schneider
 */
public class MapWriterPipe<T, K, V> extends TerminalPipe {
  private final Pipe<T> input;
  private final Map<K, V> outputMap;
  private final FailableFunction<T, K, ValidationPipeException> keyExtractor;
  private final FailableFunction<T, V, ValidationPipeException> valueExtractor;

  /**
   * Constructor
   *
   * Creates a writer that populates a new {@link java.util.HashMap}.
   * @param inputPipe The input pipe to consume
   * @param keyExtractor The logic for extracting the map key from an input item. In case that two different items produce the same key, the latter (in input pipe appearance order)
   *                     will override the results of the former.
   * @param valueExtractor  The logic for extracting the map value from an input item
   */
  public MapWriterPipe(Pipe<T> inputPipe, FailableFunction<T, K, ValidationPipeException> keyExtractor, FailableFunction<T, V, ValidationPipeException> valueExtractor) {
    this(inputPipe, keyExtractor, valueExtractor, new HashMap<>());
  }

  /**
   * Constructor
   *
   * @param inputPipe The input pipe to consume
   * @param keyExtractor The logic for extracting the map key from an input item. In case that two different items produce the same key, the latter (in input pipe appearance order)
   *                     will override the results of the former.
   * @param valueExtractor  The logic for extracting the map value from an input item
   * @param outputMap The map to populate
   */
  public MapWriterPipe(Pipe<T> inputPipe, FailableFunction<T, K, ValidationPipeException> keyExtractor, FailableFunction<T, V, ValidationPipeException> valueExtractor, Map<K, V> outputMap) {
    this.input = inputPipe;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.outputMap = outputMap;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();

    T item;
    while ((item = input.next()) != null) {
      K key = keyExtractor.apply(item);
      V value = valueExtractor.apply(item);
      outputMap.put(key, value);
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  /**
   * @return The output map
   */
  public Map<K, V> getItems() {
    return outputMap;
  }
}
