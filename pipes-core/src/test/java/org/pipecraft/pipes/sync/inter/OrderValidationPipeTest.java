package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.ConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * @author Eyal Rubichi
 */
public class OrderValidationPipeTest {

  /**
   * Flow: validate ordering of an ordered pipe
   * Expected result:
   * 1. pipe terminates without exception
   * 2. all elements were consumed
   * 3. consuming the elements in the right order
   */
  @Test
  public void startOrdered() throws Exception {
    List<Integer> collectedItems = new ArrayList<>();
    List<Integer> inputItems = Arrays.asList(5, 4, 3, 2, 1);
    Pipe<Integer> testDataPipe = new CollectionReaderPipe<>(inputItems);
    OrderValidationPipe<Integer> validatingPipe = new OrderValidationPipe<>(testDataPipe, Comparator.reverseOrder());
    try (TerminalPipe collectorPipe = new ConsumerPipe<>(validatingPipe, collectedItems::add)) {
      collectorPipe.start();
    }

    assertEquals(inputItems.size(), collectedItems.size());

    for (int i = 0; i < inputItems.size(); i++) {
      assertEquals(inputItems.get(i), collectedItems.get(i));
    }
  }

  /**
   * Flow: validate ordering of a non ordered pipe
   * Expected result:
   * 1. pipe terminates with validation exception
   */
  @Test
  public void startNotOrdered() {
    assertThrows(ValidationPipeException.class, () -> {
      Pipe<Integer> testDataPipe = new CollectionReaderPipe<>(5, 4, 5, 2, 1);
      OrderValidationPipe<Integer> validatingPipe = new OrderValidationPipe<>(testDataPipe, Comparator.reverseOrder());
      try (ConsumerPipe<Integer> consumerPipe = new ConsumerPipe<>(validatingPipe)) {
        consumerPipe.start();
      }
    });
  }
}
