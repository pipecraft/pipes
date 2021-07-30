package org.pipecraft.pipes.sync.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.StringReader;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.infra.io.FileUtils;

/**
 * Tests source pipes
 * 
 * @author Eyal Schneider
 */
public class SourcePipesTest {

  @Test
  public void testReaderPipe() throws Exception {
    StringReader sr = new StringReader("a\nb\nc");
    try (ReaderPipe rp = new ReaderPipe(sr)) {
      rp.start();
      assertEquals("a", rp.next());
      assertEquals("b", rp.next());
      assertEquals("c", rp.next());
      assertNull(rp.next());
    }
  }
  
  @Test
  public void testReaderPipeEmpty() throws Exception {
    StringReader sr = new StringReader("");
    try(ReaderPipe rp = new ReaderPipe(sr)) {
      rp.start();
      assertNull(rp.next());
    }
  }

  @Test
  public void testListPipe() throws Exception {
    try (CollectionReaderPipe<String> p = new CollectionReaderPipe<>(Lists.newArrayList("a", "b", "c"))) {
      p.start();
      assertEquals("a", p.next());
      assertEquals("b", p.next());
      assertEquals("c", p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testListPipeEmpty() throws Exception {
    try (CollectionReaderPipe<String> p = new CollectionReaderPipe<>(Collections.emptyList())) {
      p.start();
      assertNull(p.next());
    }
  }

  @Test
  public void testTxtFileReaderPipe() throws Exception {
    File f = FileUtils.createTempFile("test",".txt");
    FileUtils.writeCSV(Lists.newArrayList(Collections.singletonList("a"), Collections.singletonList("b"), Collections.singletonList("c")), f);
    
    try (TxtFileReaderPipe p = new TxtFileReaderPipe(f)) {
      p.start();
      assertEquals("a", p.next());
      assertEquals("b", p.next());
      assertEquals("c", p.next());
      assertNull(p.next());
    }
  }
  
  @Test
  public void testTxtFileReaderPipeEmpty() throws Exception {
    File f = FileUtils.createTempFile("test",".txt");
    FileUtils.writeCSV(Collections.emptyList(), f);
    
    try(TxtFileReaderPipe p = new TxtFileReaderPipe(f)) {
      p.start();
      assertNull(p.next());
    }
  }  

  @Test
  public void testSeqGenPipe() throws Exception {
    try (SeqGenPipe<Integer> p = new SeqGenPipe<>(i -> (i < 3)? 10 * i.intValue() : null)) {
      p.start();
      assertEquals(0, p.next());
      assertEquals(10, p.next());
      assertEquals(20, p.next());
      assertNull(p.next());
    }
  }

  @Test
  public void testSeqGenPipeEmpty() throws Exception {
    try (SeqGenPipe<Integer> p = new SeqGenPipe<>(i -> null)) {
      p.start();
      assertNull(p.next());
    }
  }
}
