package org.pipecraft.pipes.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link TxtItemEncoder} and {@link TxtItemDecoder}
 * 
 * @author Eyal Schneider
 */
public class TxtCodecTest {
  @ParameterizedTest
  @ValueSource(strings = { "Hello there!", ""})
  public void testDecodeByteArray(String s) throws Exception {
    String decoded = new TxtDecoderFactory<>(v -> v).newByteArrayDecoder().decode(s.getBytes("UTF8"));
    assertEquals(s, decoded);
  }
  
  @ParameterizedTest
  @ValueSource(strings = { "UTF8", "UTF16"})
  public void testDecodeByteArrayWithCharsets(String charset) throws Exception {
    String s = "Hello there!";
    String decoded = new TxtDecoderFactory<>(v -> v, Charset.forName(charset)).newByteArrayDecoder()
        .decode(s.getBytes(charset));
    assertEquals(s, decoded);
  }

  @Test
  public void testDecodeByteArrayWithTransformation() throws Exception {
    String s = "156";
    Integer decoded = new TxtDecoderFactory<>(Integer::parseInt).newByteArrayDecoder()
        .decode(s.getBytes("UTF8"));
    assertEquals(156, decoded);
  }

  @ParameterizedTest
  @ValueSource(strings = { "UTF8", "UTF16"})
  public void testDecodeISWithCharsets(String charset) throws Exception {
    String s = "Hello there1\nHello again";
    byte[] bytes = s.getBytes(charset);
    try (
        InputStream is = new ByteArrayInputStream(bytes);
        ItemDecoder<String> decoder = new TxtDecoderFactory<>(v -> v, Charset.forName(charset)).newDecoder(is)) {
      assertEquals("Hello there1", decoder.decode());
      assertEquals("Hello again", decoder.decode());
      assertNull(decoder.decode());
    }
  }

  @Test
  public void testDecodeISWithTransformation() throws Exception {
    String s = "11\n22\n23";
    byte[] bytes = s.getBytes("UTF8");
    try (
        InputStream is = new ByteArrayInputStream(bytes);
        ItemDecoder<Integer> decoder = new TxtDecoderFactory<>(Integer::parseInt).newDecoder(is)) {
      assertEquals(11, decoder.decode());
      assertEquals(22, decoder.decode());
      assertEquals(23, decoder.decode());
      assertNull(decoder.decode());
    }
  }

}
