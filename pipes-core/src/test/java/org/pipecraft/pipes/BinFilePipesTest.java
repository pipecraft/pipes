package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.serialization.ItemDecoder;
import org.pipecraft.pipes.serialization.ItemEncoder;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.BinFileWriterPipe;

/**
 * Tests {@link BinInputReaderPipe} and {@link BinFileWriterPipe} classes
 * 
 * @author Eyal Schneider
 */
public class BinFilePipesTest {

  @Test
  public void testWrite() throws Exception {
    File f = FileUtils.createTempFile("test",".bin");
    
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("1", "2", "4", "7");
        BinFileWriterPipe<String> p = new BinFileWriterPipe<>(p0, f, ByteEncoder::new);

    ) {
      p.start();
    }
    
    FileInputStream fis = new FileInputStream(f);
    byte[] buffer = new byte[5];
    assertEquals(4, IOUtils.read(fis, buffer));
    assertArrayEquals(new byte[]{1, 2, 4, 7}, Arrays.copyOf(buffer, 4));
  }

  @Test
  public void testRead() throws Exception {
    File f = FileUtils.createTempFile("test",".bin");
    FileOutputStream fos = new FileOutputStream(f);
    fos.write(new byte[]{1, 2, 4, 7});
    fos.close();
    
    try (BinInputReaderPipe<String> p = new BinInputReaderPipe<>(f, ByteDecoder::new)) {
      p.start();
      assertEquals("1", p.next());
      assertEquals("2", p.next());
      assertEquals("4", p.next());
      assertEquals("7", p.next());
      assertNull(p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testReadEmpty() throws Exception {
    File f = FileUtils.createTempFile("test",".bin");
    
    try (BinInputReaderPipe<String> p = new BinInputReaderPipe<>(f, ByteDecoder::new)) {
      p.start();
      assertNull(p.next());
    }
  }
  
  @Test
  public void testWriteRead() throws Exception {
    File f = FileUtils.createTempFile("test",".bin");
    
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("1", "2", "4", "7");
        BinFileWriterPipe<String> p = new BinFileWriterPipe<>(p0, f, new FileWriteOptions().setCompression(Compression.GZIP), ByteEncoder::new);
    ) {
      p.start();
    }
    
    try (BinInputReaderPipe<String> p = new BinInputReaderPipe<>(f, new FileReadOptions().setCompression(Compression.GZIP), ByteDecoder::new)) {
      p.start();
      assertEquals("1", p.next());
      assertEquals("2", p.next());
      assertEquals("4", p.next());
      assertEquals("7", p.next());
      assertNull(p.next());
      assertNull(p.next());
    }
  }
  
  private static class ByteEncoder implements ItemEncoder<String> {
    private final BufferedOutputStream os;

    public ByteEncoder(OutputStream os, FileWriteOptions options) throws IOException {
      this.os = FileUtils.getOutputStream(os, options);
    }
    
    @Override
    public void encode(String item) throws IOException {
      os.write(Byte.parseByte(item));
    }
    
    @Override
    public void close() throws IOException {
      os.close();
    }
  }
  
  private static class ByteDecoder implements ItemDecoder<String> {
    private final BufferedInputStream is;

    public ByteDecoder(InputStream is, FileReadOptions options) throws IOException {
      this.is = FileUtils.getInputStream(is, options);
    }
    
    @Override
    public String decode() throws IOException {
      int v = is.read();
      if (v == -1) {
        return null;
      }
      return String.valueOf(v);
    }
    
    @Override
    public void close() throws IOException {
      is.close();
    }
  }

}
