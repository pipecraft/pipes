package org.pipecraft.pipes.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.pipecraft.pipes.exceptions.ValidationPipeException;

public class AvroCodecTest {
  @Test
  public void testEncodeDecodeIS() throws IOException {
    List<TestInfo> data =
        Arrays.asList(new TestInfo(1, "zacharya is the kinggggggg"), new TestInfo(2, "is he?"));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (
        AvroEncoder<TestInfo> encoder = new AvroEncoder<>(TestInfo.class, os)) {
      for (TestInfo obj : data) {
        encoder.encode(obj);
      }
    }

    try (
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        AvroDecoder<TestInfo> decoder = new AvroDecoder<>(TestInfo.class, is)) {
      assertEquals(data.get(0), decoder.decode());
      assertEquals(data.get(1), decoder.decode());
      assertNull(decoder.decode());
    }
  }

  @Test
  public void testEncodeDecodeByteArray() throws IOException, ValidationPipeException {
    TestInfo data =
        new TestInfo(1, "zacharya is my man");
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (
        AvroEncoder<TestInfo> encoder = new AvroEncoder<>(TestInfo.class, os)) {
      encoder.encode(data);
    }

    byte[] bytes = os.toByteArray();
    ByteArrayDecoder<TestInfo> decoder = AvroDecoder.getFactory(TestInfo.class).newByteArrayDecoder();
    TestInfo decoded = decoder.decode(bytes);
    assertEquals(data, decoded);
  }

  @Test
  public void testEOF() throws IOException {
    TestInfo data = new TestInfo(1, "WHOAMI?");
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (
        AvroEncoder<TestInfo> encoder = new AvroEncoder<>(TestInfo.class, os)) {
      encoder.encode(data);
    }
    byte[] encoded = os.toByteArray();
    byte[] partial = Arrays.copyOfRange(encoded, 0, encoded.length / 2);
    try (
        ByteArrayInputStream is = new ByteArrayInputStream(partial);
        AvroDecoder<TestInfo> decoder = new AvroDecoder<>(TestInfo.class, is)) {
      Assertions.assertThrows(EOFException.class, decoder::decode);
    }
  }
  
  static class TestInfo {
    private final int id;
    private final String name;

    public TestInfo() {
      this(0, "");
    }

    public TestInfo(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return "id=" + getId() + ",name=" + getName();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TestInfo)) {
        return false;
      }

      TestInfo other = (TestInfo) o;
      return other.getId() == this.getId() && other.getName().equals(this.getName());
    }
  }
}
