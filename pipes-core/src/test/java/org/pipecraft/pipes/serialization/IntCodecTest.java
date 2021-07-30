package org.pipecraft.pipes.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.BinFileWriterPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests {@link IntDecoders} and {@link IntEncoders}
 *
 * @author Eyal Schneider
 */
public class IntCodecTest {

  private static Stream<Arguments> provideIntCodecPairs() {
    return Stream.of(
        Arguments.of(IntEncoders.INT16_BE_ENCODER_FACTORY, IntDecoders.INT16_BE_DECODER_FACTORY),
        Arguments.of(IntEncoders.INT16_LE_ENCODER_FACTORY, IntDecoders.INT16_LE_DECODER_FACTORY),
        Arguments.of(IntEncoders.INT32_BE_ENCODER_FACTORY, IntDecoders.INT32_BE_DECODER_FACTORY),
        Arguments.of(IntEncoders.INT32_LE_ENCODER_FACTORY, IntDecoders.INT32_LE_DECODER_FACTORY)
    );
  }

  private static Stream<Arguments> provideLongCodecPairs() {
    return Stream.of(
        Arguments.of(IntEncoders.INT64_BE_ENCODER_FACTORY, IntDecoders.INT64_BE_DECODER_FACTORY),
        Arguments.of(IntEncoders.INT64_LE_ENCODER_FACTORY, IntDecoders.INT64_LE_DECODER_FACTORY)
    );
  }

  @ParameterizedTest
  @MethodSource("provideIntCodecPairs")
  public void testUpTo32BitInts(EncoderFactory<Integer> encFactory, DecoderFactory<Integer> decFactory) throws Exception {
    File file = FileUtils.createTempFile("testIntCodec", ".bin");
    try {
      List<Integer> values = List.of(1, 4, 1025);
      List<Integer> decodedValues = new ArrayList<>();
      // Writer flow
      try (
          Pipe<Integer> collWriterP = new CollectionReaderPipe<>(values);
          TerminalPipe writerP = new BinFileWriterPipe<>(collWriterP, file, encFactory)
      ) {
        writerP.start();
      }

      // Reader flow
      try (
          Pipe<Integer> readerP = new BinInputReaderPipe<>(file, decFactory);
          TerminalPipe collReaderP = new CollectionWriterPipe<>(readerP, decodedValues)
      ) {
        collReaderP.start();
      }
      assertEquals(values, decodedValues);
    } finally {
      FileUtils.deleteFiles(file);
    }
  }

  @ParameterizedTest
  @MethodSource("provideLongCodecPairs")
  public void test64IntBits(EncoderFactory<Long> encFactory, DecoderFactory<Long> decFactory) throws Exception {
    File file = FileUtils.createTempFile("testIntCodec", ".bin");
    try {
      List<Long> values = List.of(1L, 4L, Long.MAX_VALUE);
      List<Long> decodedValues = new ArrayList<>();
      // Writer flow
      try (
          Pipe<Long> collWriterP = new CollectionReaderPipe<>(values);
          TerminalPipe writerP = new BinFileWriterPipe<>(collWriterP, file, encFactory)
      ) {
        writerP.start();
      }

      // Reader flow
      try (
          Pipe<Long> readerP = new BinInputReaderPipe<>(file, decFactory);
          TerminalPipe collReaderP = new CollectionWriterPipe<>(readerP, decodedValues)
      ) {
        collReaderP.start();
      }
      assertEquals(values, decodedValues);
    } finally {
      FileUtils.deleteFiles(file);
    }
  }

}
