package org.pipecraft.pipes.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link CSVEncoder} and {@link CSVDecoder}
 */
public class CSVCodecTest {

  @Test
  public void testRead() throws Exception {
    DecoderFactory<TestTuple> factory = CSVDecoder.getFactory(TestTuple::fromCSVColumns); // Verifying that extracting the decoder using a factory works well

    String input = "\"a \",1\nb ,\" 2\"\n\"c,d\",3\n, 4 ";
    byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    try (ItemDecoder<TestTuple> decoder = factory.newDecoder(is)) {
      TestTuple r = decoder.decode();
      assertEquals(new TestTuple("a ", 1), r);
      r = decoder.decode();
      assertEquals(new TestTuple("b ", 2), r);
      r = decoder.decode();
      assertEquals(new TestTuple("c,d", 3), r);
      r = decoder.decode();
      assertEquals(new TestTuple(null, 4), r); // Empty columns are parsed as null
      assertNull(decoder.decode());
    }
  }

  @Test
  public void testReadEmptyValues() throws Exception {
    DecoderFactory<TestTuple> factory = CSVDecoder.getFactory(TestTuple::fromCSVColumns); // Verifying that extracting the decoder using a factory works well

    String input = "\"\",1\n,\"2\""; //Quoted empty string should be read as empty string, while missing value should be read as null
    byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    try (ItemDecoder<TestTuple> decoder = factory.newDecoder(is)) {
      TestTuple r = decoder.decode();
      assertEquals(new TestTuple("", 1), r);
      r = decoder.decode();
      assertEquals(new TestTuple(null, 2), r);
      r = decoder.decode();
      assertNull(decoder.decode());
    }
  }

  // Here we test skipping header rows and using a non-standard column separator and charset encoding.
  @Test
  public void testReadNonStandardSettings() throws Exception {
    DecoderFactory<TestTuple> factory = CSVDecoder.getFactory(TestTuple::fromCSVColumns, StandardCharsets.UTF_16, '_', 2);

    String input = "header1\nheader rows_ #2\n\"a \"_1\nb _\" 2\"\n\"c_d\"_3\n_ 4 ";
    byte[] bytes = input.getBytes(StandardCharsets.UTF_16);
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    try (ItemDecoder<TestTuple> decoder = factory.newDecoder(is)) {
      TestTuple r = decoder.decode();
      assertEquals(new TestTuple("a ", 1), r);
      r = decoder.decode();
      assertEquals(new TestTuple("b ", 2), r);
      r = decoder.decode();
      assertEquals(new TestTuple("c_d", 3), r);
      r = decoder.decode();
      assertEquals(new TestTuple(null, 4), r); // Empty columns are parsed as null
      assertNull(decoder.decode());
    }
  }

  @Test
  public void testWrite() throws Exception {
    EncoderFactory<TestTuple> factory = CSVEncoder.getFactory(TestTuple::toCSVColumns); // Verifying that extracting the encoder using a factory works well

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (ItemEncoder<TestTuple> encoder = factory.newEncoder(os)) {
      encoder.encode(new TestTuple("a ", 1));
      encoder.encode(new TestTuple(" b", 2));
      encoder.encode(new TestTuple("c,d", 3));
      encoder.encode(new TestTuple(null, 4));
    }
    String lines = os.toString(StandardCharsets.UTF_8);
    assertEquals("a ,1\n b,2\n\"c,d\",3\n,4\n", lines);
  }

  @Test
  public void testWriteEmptyValues() throws Exception {
    EncoderFactory<TestTuple> factory = CSVEncoder.getFactory(TestTuple::toCSVColumns); // Verifying that extracting the encoder using a factory works well

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (ItemEncoder<TestTuple> encoder = factory.newEncoder(os)) {
      encoder.encode(new TestTuple("", 1));
      encoder.encode(new TestTuple(null, 4));
    }
    String lines = os.toString(StandardCharsets.UTF_8);
    assertEquals("\"\",1\n,4\n", lines);
  }

  @Test
  public void testWriteWithHeaderBlock() throws Exception {
    EncoderFactory<TestTuple> factory = CSVEncoder.getFactory(TestTuple::toCSVColumns, StandardCharsets.UTF_8, ',', "h1\nh2,2\nh3");

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (ItemEncoder<TestTuple> encoder = factory.newEncoder(os)) {
      encoder.encode(new TestTuple("a ", 1));
      encoder.encode(new TestTuple(" b", 2));
    }
    String lines = os.toString(StandardCharsets.UTF_8);
    assertEquals("h1\nh2,2\nh3\na ,1\n b,2\n", lines);
  }

  @Test
  public void testWriteThenRead() throws Exception {
    File f = FileUtils.createTempFile("CSVCodecTest", FileUtils.CSV_EXTENSION);
    try {
      try (
          FileOutputStream os = new FileOutputStream(f);
          CSVEncoder<TestTuple> encoder = new CSVEncoder<>(os, TestTuple::toCSVColumns);
      ) {
        encoder.encode(new TestTuple("a ", 1));
        encoder.encode(new TestTuple(" b", 2));
        encoder.encode(new TestTuple("c,d", 3));
        encoder.encode(new TestTuple("", 4));
        encoder.encode(new TestTuple(null, 5));
      }

      try (
          FileInputStream is = new FileInputStream(f);
          CSVDecoder<TestTuple> decoder = new CSVDecoder<>(is, TestTuple::fromCSVColumns);
      ) {
        TestTuple r = decoder.decode();
        assertEquals(new TestTuple("a ", 1), r);
        r = decoder.decode();
        assertEquals(new TestTuple(" b", 2), r);
        r = decoder.decode();
        assertEquals(new TestTuple("c,d", 3), r);
        r = decoder.decode();
        assertEquals(new TestTuple("", 4), r);
        r = decoder.decode();
        assertEquals(new TestTuple(null, 5), r);
        assertNull(decoder.decode());
      }
    } finally {
      FileUtils.deleteFiles(f);
    }
  }

  // A test touple consisting of a name (string) and a count (int).
  // the column transformation methods here preserve leading/trailing spaces in the name field,
  // but not in the age field
  private static class TestTuple {
    private final String name;
    private final int count;

    public TestTuple(String name, int count) {
      this.name = name;
      this.count = count;
    }

    public String getName() {
      return name;
    }

    public int getCount() {
      return count;
    }

    public String[] toCSVColumns() {
      return new String[] {name, String.valueOf(count)};
    }

    public static TestTuple fromCSVColumns(String[] columns) throws ValidationPipeException {
      try {
        return new TestTuple(columns[0], Integer.parseInt(columns[1].trim()));
      } catch (NumberFormatException e) {
        throw new ValidationPipeException(e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestTuple testTuple = (TestTuple) o;
      return count == testTuple.count && Objects.equals(name, testTuple.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, count);
    }

    @Override
    public String toString() {
      return "TestTuple{" +
          "name='" + name + '\'' +
          ", count=" + count +
          '}';
    }
  }
}
