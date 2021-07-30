package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

import org.junit.jupiter.api.Test;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.serialization.TxtDecoderFactory;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.pipes.sync.inter.SortPipe;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;

/**
 * Tests {@link SortPipe}, which sorts an input pipe, possibly using local disk for intermediate results
 * 
 * @author Eyal Schneider
 */
public class SortPipeTest {

  @Test
  public void testEmpty() throws Exception {
    
    try (
        SortPipe<String> sp = new SortPipe<>(EmptyPipe.instance(), 100, null, null, null, String::compareTo);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertTrue(lwp.getItems().isEmpty());
    }
  }

  @Test
  public void testInMemory() throws Exception {
    
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("h", "a", "c", "d", "b");
        SortPipe<String> sp = new SortPipe<>(p0, 100, null, null, null, String::compareTo);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertEquals(Arrays.asList("a", "b", "c", "d", "h"), lwp.getItems());
    }
  }

  @Test
  public void testInMemoryExactlyOnLimit() throws Exception {
    
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("h", "a", "c", "d", "b");
        SortPipe<String> sp = new SortPipe<>(p0, 5, null, null, null, String::compareTo);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertEquals(Arrays.asList("a", "b", "c", "d", "h"), lwp.getItems());
    }
  }

  @Test
  public void testWithDiskUsage() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("test");
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("i", "a", "c", "d", "b", "a", "h", "b");
        SortPipe<String> sp = new SortPipe<>(p0, 5, tmpFolder, new TxtEncoderFactory<>() , new TxtDecoderFactory<>(v->v), String::compareTo);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertEquals(Arrays.asList("a", "a", "b", "b", "c", "d", "h", "i"), lwp.getItems());
    }
  }

  @Test
  public void testWithDiskUsageNoCompression() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("test");
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("i", "a", "c", "d", "b", "a", "h", "b");
        SortPipe<String> sp = new SortPipe<>(p0, 5, tmpFolder, new TxtEncoderFactory<>() , new TxtDecoderFactory<>(v->v), String::compareTo, Compression.NONE);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertEquals(Arrays.asList("a", "a", "b", "b", "c", "d", "h", "i"), lwp.getItems());
    }
  }

  @Test
  public void testDiskDisabled() throws Exception {
    
    try (CollectionReaderPipe<String> p0 = new CollectionReaderPipe<>("h", "a", "c", "d", "b");
        SortPipe<String> sp = new SortPipe<>(p0, String::compareTo);
        CollectionWriterPipe<String> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertEquals(Arrays.asList("a", "b", "c", "d", "h"), lwp.getItems());
    }
  }
  
  @Test
  public void testInconsistentComparatorWithEquals() throws Exception {
    File tmpFolder = FileUtils.createTempFolder("test");
    try (
        CollectionReaderPipe<Fruit> p0 = new CollectionReaderPipe<>(
            new Fruit("Banana", 20), new Fruit("Apple", 15), new Fruit("Pear", 20), new Fruit("Grapes", 2), new Fruit("Apple", 15) 
        );
        SortPipe<Fruit> sp = new SortPipe<>(p0, 3, tmpFolder, new TxtEncoderFactory<>(Fruit::toCSV) , new TxtDecoderFactory<>(
            Fruit::fromCSV), Comparator.comparing(Fruit::getCount));
        CollectionWriterPipe<Fruit> lwp = new CollectionWriterPipe<>(sp);) {
      lwp.start();
      assertTrue(
          Arrays.asList(
              new Fruit("Grapes", 2), new Fruit("Apple", 15), new Fruit("Apple", 15), new Fruit("Banana", 20), new Fruit("Pear", 20)
          ).equals(lwp.getItems()) || 
          Arrays.asList(
              new Fruit("Grapes", 2), new Fruit("Apple", 15), new Fruit("Apple", 15), new Fruit("Pear", 20), new Fruit("Banana", 20)
          ).equals(lwp.getItems()));
    }
  }

  private static class Fruit {
    private final String name;
    private final int count;
    
    public Fruit(String name, int count) {
      this.name = name;
      this.count = count;
    }

    public int getCount() {
      return count;
    }

    public String toCSV() {
      return name + "," + count;
    }

    public static Fruit fromCSV(String csv) {
      String[] parts = csv.split(",");
      return new Fruit(parts[0], Integer.parseInt(parts[1]));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + count;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Fruit other = (Fruit) obj;
      if (count != other.count)
        return false;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      return true;
    }
  }
}
