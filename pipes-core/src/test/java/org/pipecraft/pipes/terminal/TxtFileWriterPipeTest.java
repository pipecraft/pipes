package org.pipecraft.pipes.terminal;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.sync.source.CollectionReaderPipe;

/**
 * Tests {@link TxtFileWriterPipe}
 *
 * @author Eyal Schneider
 */
public class TxtFileWriterPipeTest {

  @Test
  public void writeWithDefaultOptions() throws Exception {
    List<String> inputItems = asList("1", "2", "3");
    File outputFile = FileUtils
        .createTempFile(TxtFileWriterPipeTest.class.getSimpleName(), FileUtils.CSV_EXTENSION);

    try (
        Pipe<String> genP = new CollectionReaderPipe<>(inputItems);
        TxtFileWriterPipe writerPipe = new TxtFileWriterPipe(genP, outputFile)) {
      writerPipe.start();
    }
    assertEquals(FileUtils.getLinesFromFile(outputFile), inputItems);
  }

  @Test
  public void writeCompressedUTF16() throws Exception {
    List<String> inputItems = asList("1", "2", "3");
    File outputFile = FileUtils.createTempFile(TxtFileWriterPipeTest.class.getSimpleName(), ".some_extension");

    try (
        Pipe<String> genP = new CollectionReaderPipe<>(inputItems);
        TxtFileWriterPipe writerPipe = new TxtFileWriterPipe(genP, outputFile, StandardCharsets.UTF_16, new FileWriteOptions().setCompression(
            Compression.ZSTD))) {
      writerPipe.start();
    }

    assertEquals(FileUtils.getLinesFromFile(outputFile, StandardCharsets.UTF_16, new FileReadOptions().setCompression(Compression.ZSTD)), inputItems);
  }
}