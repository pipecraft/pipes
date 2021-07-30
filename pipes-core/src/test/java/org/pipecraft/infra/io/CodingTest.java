package org.pipecraft.infra.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

/**
 * @author eyal
 */
public class CodingTest {

  @Test
  public void testVarintSize() {
    assertEquals(1, Coding.computeVarint32Size(0x0000007F));
    assertEquals(2, Coding.computeVarint32Size(0x000000FF));
    assertEquals(3, Coding.computeVarint32Size(0x00007FFF));
    assertEquals(3, Coding.computeVarint32Size(0x001FFFFF));
    assertEquals(4, Coding.computeVarint32Size(0x003FFFFF));
    assertEquals(4, Coding.computeVarint32Size(0x0FFFFFFF));
    assertEquals(5, Coding.computeVarint32Size(0x1FFFFFFF));
    assertEquals(5, Coding.computeVarint32Size(0xFFFFFFFF));
  }
  
  @Test
  public void testVarintEncodeDecodeEdgeCases() throws IOException {
    int[] encodedVals = new int[]{0x0000007F, 0x000000FF, 0x00007FFF, 0x001FFFFF, 0x003FFFFF, 0x0FFFFFFF, 0x1FFFFFFF, 0xFFFFFFFF};
    int[] varintLengths = new int[] {1, 2, 3, 3, 4, 4, 5, 5};
    for (int i = 0; i < encodedVals.length; i++) {
      int v = encodedVals[i];
      byte[] buffer = new byte[5];
      Coding.writeVarint32(v, buffer, 0);
      ByteArrayInputStream is = new ByteArrayInputStream(buffer);
      MutableInt offset = new MutableInt();
      assertEquals(v, Coding.readVarint32(is, offset));
      assertEquals(varintLengths[i], offset.intValue());
    }
  }
  
  @Test
  public void testVarintEncodeDecodeFromInputStream() throws IOException {
    for (int v = 0; v < 1000_000; v++) {
      byte[] buffer = new byte[5];
      Coding.writeVarint32(v, buffer, 0);
      ByteArrayInputStream is = new ByteArrayInputStream(buffer);
      MutableInt offset = new MutableInt();
      assertEquals(v, Coding.readVarint32(is, offset));
    }
  }

  @Test
  public void testVarintEncodeDecodeFromBuffer() throws IOException {
    for (int v = 0; v < 1000_000; v++) {
      byte[] buffer = new byte[5];
      Coding.writeVarint32(v, buffer, 0);
      MutableInt offset = new MutableInt();
      assertEquals(v, Coding.readVarint32(buffer, offset));
    }
  }

  @Test
  public void testBadCoding() {
    byte[] buffer = new byte[5];
    Arrays.fill(buffer, (byte)-1);
    ByteArrayInputStream is = new ByteArrayInputStream(buffer);
    assertThrows(IOException.class, () -> Coding.readVarint32(is, new MutableInt()));
  }

  @Test
  public void testWriteRead32BitLE() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(); 
    Coding.writeLittleEndian32(123456, os);
    
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    int v = Coding.readLittleEndian32(is);
    
    assertEquals(123456, v);
  }

  @Test
  public void testWriteRead32BitBE() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(); 
    Coding.writeBigEndian32(123456, os);
    
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    int v = Coding.readBigEndian32(is);
    
    assertEquals(123456, v);
  }

  @Test
  public void testWriteRead64BitLE() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(); 
    Coding.writeLittleEndian64(1234567890123L, os);
    
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    long v = Coding.readLittleEndian64(is);
    
    assertEquals(1234567890123L, v);
  }

  @Test
  public void testWriteRead64BitBE() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(); 
    Coding.writeBigEndian64(1234567890123L, os);
    
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    long v = Coding.readBigEndian64(is);
    
    assertEquals(1234567890123L, v);
  }

  @Test
  public void testReadBytesFullySuccess() throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3});
    byte[] bytesRead = Coding.read(is, 2);
    
    assertArrayEquals(new byte[] {1, 2}, bytesRead);
  }

  @Test
  public void testReadBytesFullyFailure() {
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3});
    assertThrows(EOFException.class, () -> Coding.read(is, 4));
  }

  @Test
  public void testTryReadBytesSuccess() throws IOException {
    byte[] buffer = new byte[3];
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3});
    assertTrue(Coding.tryRead(is, buffer, 0, 2));
    assertArrayEquals(new byte[] {1, 2, 0}, buffer);
  }

  @Test
  public void testTryReadBytesSuccessEOF() throws IOException {
    byte[] buffer = new byte[3];
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {});
    assertFalse(Coding.tryRead(is, buffer, 0, 2));
    assertArrayEquals(new byte[] {0, 0, 0}, buffer);
  }

  @Test
  public void testTryReadBytesFailurePrematureEOF() {
    byte[] buffer = new byte[4];
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3});
    assertThrows(EOFException.class, () -> Coding.tryRead(is, buffer, 0, 4));
  }
}
