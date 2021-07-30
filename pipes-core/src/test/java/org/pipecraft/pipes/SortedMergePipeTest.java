package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.exceptions.OutOfOrderPipeException;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.sync.inter.SortedMergePipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;

/**
 * Tests the pipe performing sort-merge on pipes
 * 
 * @author Eyal Schneider
 */
public class SortedMergePipeTest {

  @Test
  public void testPartialMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(1, 2, 3, 4, 5, 6);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 4, 6, 8, 10, 10);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p);
        ) {
      tp.start();
      assertEquals(Lists.newArrayList(1, 2, 2, 3, 4, 4, 5, 6 ,6, 8, 10, 10), tp.getItems());
    }
  }
  
  @Test
  public void testPrefixMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4, 5, 6, 7);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 3);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertEquals(Lists.newArrayList(2, 2, 3, 3, 4, 5, 6 ,7), tp.getItems());
    }
  }

  @Test
  public void testSuffixMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4, 5, 6, 7);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(6, 7);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertEquals(Lists.newArrayList(2, 3, 4, 5, 6 ,6, 7, 7), tp.getItems());
    }
  }

  @Test
  public void testFullMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(2, 3, 4);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertEquals(Lists.newArrayList(2, 2, 3, 3, 4, 4), tp.getItems());
    }
  }

  @Test
  public void testNoMatch() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(1, 5, 6);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6), tp.getItems());
    }
  }
  
  @Test
  public void testSingleInput() throws Exception {
    CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
    try (
        Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1);
        CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertEquals(Lists.newArrayList(2, 3, 4), tp.getItems());
    }
  }

  @Test
  public void testNoInput() throws Exception {
    try (
      Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo);
      CollectionWriterPipe<Integer> tp = new CollectionWriterPipe<>(p)) {
    tp.start();
    assertEquals(Lists.newArrayList(), tp.getItems());
  }
  }

  @Test
  public void testOutOfOrder() throws Exception {
    assertThrows(OutOfOrderPipeException.class, () -> {
      CollectionReaderPipe<Integer> p1 = new CollectionReaderPipe<>(2, 3, 4);
      CollectionReaderPipe<Integer> p2 = new CollectionReaderPipe<>(1, 3, 9, 6);
      try (Pipe<Integer> p = new SortedMergePipe<>(Integer::compareTo, p1, p2)) {
        p.start();
        while (p.peek() != null) {
          p.next();
        }
      }
    });
  }

  @Test
  public void testInconsistentComparatorWithEquals() throws Exception {
    CollectionReaderPipe<Fruit> p1 = new CollectionReaderPipe<>(new Fruit("Pear", 3), new Fruit("Apple", 5), new Fruit("Orange", 5));
    CollectionReaderPipe<Fruit> p2 = new CollectionReaderPipe<>(new Fruit("Apple", 2), new Fruit("Pear", 3), new Fruit("Orange", 20), new Fruit("Banana", 50));
    try (
        Pipe<Fruit> p = new SortedMergePipe<>(Comparator.comparing(Fruit::getCount), p1, p2);
        CollectionWriterPipe<Fruit> tp = new CollectionWriterPipe<>(p)) {
      tp.start();
      assertTrue(
          tp.getItems().equals(
              Lists.newArrayList(new Fruit("Apple", 2), new Fruit("Pear", 3), new Fruit("Pear", 3), new Fruit("Apple", 5), new Fruit("Orange", 5), new Fruit("Orange", 20), new Fruit("Banana", 50))) ||
          tp.getItems().equals(
              Lists.newArrayList(new Fruit("Apple", 2), new Fruit("Pear", 3), new Fruit("Pear", 3), new Fruit("Orange", 5), new Fruit("Apple", 5), new Fruit("Orange", 20), new Fruit("Banana", 50)))
          );
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
