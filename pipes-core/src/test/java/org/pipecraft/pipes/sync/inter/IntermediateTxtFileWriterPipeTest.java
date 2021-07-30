package org.pipecraft.pipes.sync.inter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;

public class IntermediateTxtFileWriterPipeTest {

  @Test
  public void writeToFile() throws Exception {
    List<String> inputItems = IntStream.range(0, 1000).boxed().map(Object::toString).collect(Collectors.toList());
    File outputFile = FileUtils
        .createTempFile(IntermediateTxtFileWriterPipeTest.class.getSimpleName(), ".csv");
    try(
        Pipe<String> p0 = new CollectionReaderPipe<>(inputItems);
        Pipe<String> writerPipe = new IntermediateTxtFileWriterPipe<>(p0, outputFile, FailableFunction
            .identity());
        ) {
      Collection<String> actualPipeOutput;
      try (CollectionWriterPipe<String> listPipe = new CollectionWriterPipe<>(writerPipe)) {
        listPipe.start();
        actualPipeOutput = listPipe.getItems();
      }
      assertEquals(inputItems, actualPipeOutput);
      assertEquals(FileUtils.getLinesFromFile(outputFile), inputItems);
    } finally {
      FileUtils.deleteFiles(outputFile);
    }
  }
}