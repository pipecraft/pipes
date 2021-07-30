package org.pipecraft.pipes.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringWriter;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

public class WriterPipeTest {

  @Test
  public void testWriterPipe() throws Exception {
    StringWriter w = new StringWriter();
    try (
        CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>(Lists.newArrayList("10", "20", "30"));
        WriterPipe p = new WriterPipe(p0, w);
    ) {
      p.start();
    }

    assertEquals("10\n20\n30\n", w.toString());
  }
}