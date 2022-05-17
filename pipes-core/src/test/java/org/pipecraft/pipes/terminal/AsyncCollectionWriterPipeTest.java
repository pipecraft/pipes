package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.source.AsyncEmptyPipe;
import org.pipecraft.pipes.async.source.AsyncSeqGenPipe;

/**
 * Tests {@link AsyncCollectionWriterPipe}
 *
 * @author Eyal Schneider
 */
public class AsyncCollectionWriterPipeTest {

  @Test
  public void testEmpty() throws Exception {
    Set<Integer> output = Collections.synchronizedSet(new HashSet<>());
    try (
        AsyncPipe<Integer> prod = new AsyncEmptyPipe<>();
        AsyncCollectionWriterPipe<Integer> consumer = new AsyncCollectionWriterPipe<>(prod, output)) {
      consumer.start();
    }

    assertEquals(0, output.size());
  }

  @Test
  public void testNonEmptyProvidedCollection() throws Exception {
    Set<Integer> output = Collections.synchronizedSet(new HashSet<>());
    try (
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(100, Long::intValue, 4);
        AsyncCollectionWriterPipe<Integer> consumer = new AsyncCollectionWriterPipe<>(prod, output)) {
      consumer.start();
    }

    assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), output);
  }

  @Test
  public void testNonEmptyDefaultCollection() throws Exception {
    try (
        AsyncPipe<Integer> prod = new AsyncSeqGenPipe<>(100, Long::intValue, 4);
        AsyncCollectionWriterPipe<Integer> consumer = new AsyncCollectionWriterPipe<>(prod)) {
      consumer.start();
      assertEquals(100, consumer.getItems().size());
      assertEquals(IntStream.range(0, 100).boxed().collect(Collectors.toSet()), new HashSet<>(consumer.getItems()));
    }
  }
}
