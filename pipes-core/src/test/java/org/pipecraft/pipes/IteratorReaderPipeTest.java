package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.IteratorReaderPipe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test class for IteratorReaderPipe
 * @author Michal Rockban
 */
public class IteratorReaderPipeTest {


  @Test
  public void testRead() throws IOException, PipeException, InterruptedException {
    List<String> input = Arrays.asList("a", "b", "c", "d");

    try (IteratorReaderPipe<String> pipe = new IteratorReaderPipe<>(input.iterator())) {
      pipe.start();
      for (String s : input) {
        String result = pipe.next();
        assertEquals(s, result);
      }
    }
  }


  @Test
  public void testEmptyRead() throws IOException, PipeException, InterruptedException {
    List<String> input = new ArrayList<>();

    try (IteratorReaderPipe<String> pipe = new IteratorReaderPipe<>(input.iterator())) {
      pipe.start();
      assertNull(pipe.next());
    }
  }


  @Test
  public void testReadWithSize() throws IOException, PipeException, InterruptedException {
    List<String> input = Arrays.asList("a", "b", "c", "d");

    try (IteratorReaderPipe<String> pipe = new IteratorReaderPipe<>(input.iterator(), input.size())) {
      pipe.start();
      float count = 0;
      for (String s : input) {
        count++;
        String result = pipe.next();
        assertEquals(s, result);
        assertEquals(count/input.size(), pipe.getProgress(),0.0f);
      }
    }
  }
}
