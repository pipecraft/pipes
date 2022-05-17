package org.pipecraft.pipes.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.serialization.ProtobufTestClasses.EventRecord;

/**
 * Tests {@link ProtobufEncoder} and {@link ProtobufDecoder}
 *
 * @author Eyal Schneider
 */
public class ProtobufCodecTest {
  private final static EventRecord EVENT1 = EventRecord.newBuilder().setId(1122).setDescription("Nothing special").build();
  private final static EventRecord EVENT2 = EventRecord.newBuilder().setId(1123).setDescription("Very special").addUser(114).addUser(112).build();

  @Test
  public void testReadEmpty() throws Exception {
    File f = FileUtils.createTempFile("ProtobufCodecTest", ".proto");
    try {
      assertFileReadWithDecoder(f, new FileReadOptions()); // Asserts that no records are found
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  @ParameterizedTest
  @EnumSource(value = Compression.class, names = {"ZSTD", "GZIP", "NONE"})
  public void testRead(Compression compression) throws Exception {
    File f = FileUtils.createTempFile("ProtobufCodecTest", ".proto");
    try {
      try (OutputStream os = FileUtils.getOutputStream(f, new FileWriteOptions().setCompression(compression))) {
        EVENT1.writeDelimitedTo(os);
        EVENT2.writeDelimitedTo(os);
      }

      assertFileReadWithDecoder(f, new FileReadOptions().setCompression(compression), EVENT1, EVENT2);
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  @Test
  public void testReadCorruptData() throws Exception {
    File f = FileUtils.createTempFile("ProtobufCodecTest", ".proto");
    try {
      try (OutputStream os = FileUtils.getOutputStream(f, new FileWriteOptions())) {
        EVENT1.writeTo(os); // No delimiter
        EVENT1.writeTo(os);
      }

      assertThrows(ValidationPipeException.class, ()-> assertFileReadWithDecoder(f, new FileReadOptions(), EVENT1, EVENT2));
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  @ParameterizedTest
  @EnumSource(value = Compression.class, names = {"ZSTD", "GZIP", "NONE"})
  public void testWrite(Compression compression) throws Exception {
    File f = FileUtils.createTempFile("ProtobufCodecTest", ".proto");

    try {
      try (
          FileOutputStream os = new FileOutputStream(f);
          ItemEncoder<EventRecord> encoder = ProtobufEncoder.<EventRecord>getFactory().newEncoder(os, new FileWriteOptions().setCompression(compression))
      ) {
        encoder.encode(EVENT1);
        encoder.encode(EVENT2);
      }

      assertFileReadPlainProtobuf(f, new FileReadOptions().setCompression(compression), EVENT1, EVENT2);
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  @Test
  public void testWriteThenRead() throws Exception {
    File f = FileUtils.createTempFile("ProtobufCodecTest", ".proto");

    try {
      try (
          FileOutputStream os = new FileOutputStream(f);
          ItemEncoder<EventRecord> encoder = ProtobufEncoder.<EventRecord>getFactory().newEncoder(os, new FileWriteOptions().setCompression(Compression.GZIP))
      ) {
        encoder.encode(EVENT1);
        encoder.encode(EVENT2);
      }

      assertFileReadWithDecoder(f, new FileReadOptions().setCompression(Compression.GZIP), EVENT1, EVENT2);
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  private static void assertFileReadWithDecoder(File f, FileReadOptions readOptions, EventRecord ... expectedRecords) throws IOException, ValidationPipeException {
    try (
        FileInputStream is = new FileInputStream(f);
        ItemDecoder<EventRecord> decoder = ProtobufDecoder.getFactory(EventRecord.class).newDecoder(is, readOptions)
    ) {

      for (EventRecord expected : expectedRecords) {
        EventRecord ev = decoder.decode();
        assertEquals(expected, ev);
      }
      assertNull(decoder.decode());
    }
  }

  private static void assertFileReadPlainProtobuf(File f, FileReadOptions readOptions, EventRecord ... expectedRecords) throws IOException {
    try (InputStream is = FileUtils.getInputStream(f, readOptions)) {

      for (EventRecord expected : expectedRecords) {
        EventRecord ev = EventRecord.parseDelimitedFrom(is);
        assertEquals(expected, ev);
      }
      assertNull(EventRecord.parseDelimitedFrom(is));
    }
  }

}
