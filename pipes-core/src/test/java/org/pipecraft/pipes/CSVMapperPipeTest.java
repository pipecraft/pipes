package org.pipecraft.pipes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import org.pipecraft.pipes.sync.inter.CSVMapperPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;

/**
 * Tests {@link CSVMapperPipe}, which binds csv lines to objects
 * 
 * @author Eyal Schneider
 */
public class CSVMapperPipeTest {
  
  @Test
  public void testHeaderOnly() throws Exception {
    try (
        CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name,height, weight", "Ben,178,68", "Guy, 185 , 90");
        CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, new PersonRowParser("name", "height", "weight"));
    ) {
      csvPipe.start();
      Person p1 = csvPipe.next();
      assertEquals("Ben", p1.name);
      assertEquals(178, p1.height);
      assertEquals(68, p1.weight);
      Person p2 = csvPipe.next();
      assertEquals("Guy", p2.name);
      assertEquals(185, p2.height);
      assertEquals(90, p2.weight);
      assertNull(csvPipe.next());
      assertArrayEquals(new String[] {"name","height","weight"}, csvPipe.getColumnNames());
    }
  }
  
  @Test
  public void testHeaderAndColumnNames() throws Exception {
    try (
        CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name,height, weight", "Ben,178,68", "Guy, 185 , 90");
        CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, true, new String[] {"Name", "H", "W"}, new PersonRowParser("Name", "H", "W"));
    ) {
      csvPipe.start();
      Person p1 = csvPipe.next();
      assertEquals("Ben", p1.name);
      assertEquals(178, p1.height);
      assertEquals(68, p1.weight);
      Person p2 = csvPipe.next();
      assertEquals("Guy", p2.name);
      assertEquals(185, p2.height);
      assertEquals(90, p2.weight);
      assertNull(csvPipe.next());
      assertArrayEquals(new String[] {"Name","H","W"}, csvPipe.getColumnNames());
    }    
  }

  @Test
  public void testNonDefaultDelimiter() throws Exception {
    try (
        CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name;height; weight", "Ben;178;68", "Guy; 185 ; 90");
        CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, ';', true, null, new PersonRowParser("name", "height", "weight"));
    ) {
      csvPipe.start();
      Person p1 = csvPipe.next();
      assertEquals("Ben", p1.name);
      assertEquals(178, p1.height);
      assertEquals(68, p1.weight);
      Person p2 = csvPipe.next();
      assertEquals("Guy", p2.name);
      assertEquals(185, p2.height);
      assertEquals(90, p2.weight);
      assertNull(csvPipe.next());
    }    
  }

  @Test
  public void testSpecialChars() throws Exception {
    try (
        CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name;height; weight", "Ben the \\\"king\\\" ;178;68", "\"Guy, the best\"; 185 ; 90");
        CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, ';', true, null, new PersonRowParser("name", "height", "weight"));
    ) {
      csvPipe.start();
      Person p1 = csvPipe.next();
      assertEquals("Ben the \"king\"", p1.name);
      assertEquals(178, p1.height);
      assertEquals(68, p1.weight);
      Person p2 = csvPipe.next();
      assertEquals("Guy, the best", p2.name);
      assertEquals(185, p2.height);
      assertEquals(90, p2.weight);
      assertNull(csvPipe.next());
    }    
  }

  @Test
  public void testErrorNoColumnData() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> {
      try (
          CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(EmptyPipe.instance(), ';', false, null, new PersonRowParser("name", "height", "weight"));
      ) {
        csvPipe.start();
      }    
    });
  }

  @Test
  public void testErrorNonMatchingHeader() throws Exception {
    assertThrows(ValidationPipeException.class, () -> {
      try (
          CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name;height; weight", "Ben;178;68", "Guy; 185 ; 90");
          CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, ';', true, new String[] {"Name", "W"}, new PersonRowParser("name", "height", "weight"));
      ) {
        csvPipe.start();
      }
    });
  }

  @Test
  public void testErrorRowParsing() throws Exception {
    assertThrows(ValidationPipeException.class, () -> {
      try (
          CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name,height, weight", "Ben,178,68X");
          CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, new PersonRowParser("name", "height", "weight"));
      ) {
        csvPipe.start();
        csvPipe.next();
      }
    });
  }

  @Test
  public void testErrorDuplicateColumn() throws Exception {
    assertThrows(ValidationPipeException.class, () -> {
      try (
          CollectionReaderPipe<String> inPipe = new CollectionReaderPipe<>("name;height; weight; Height", "Ben;178;68", "Guy; 185 ; 90");
          CSVMapperPipe<Person> csvPipe = new CSVMapperPipe<>(inPipe, ';', true, null, new PersonRowParser("name", "height", "weight"));
      ) {
        csvPipe.start();
      }
    });
  }

  private static class Person {
    private final String name;
    private final int height;
    private final int weight;
    
    public Person(String name, int height, int weight) {
      this.name = name;
      this.height = height;
      this.weight = weight;
    }
  }

  private static class PersonRowParser implements FailableFunction<Map<String, String>, Person, PipeException> {
    private final String nameField;
    private final String heightField;
    private final String weightField;

    public PersonRowParser(String nameField, String heightField, String weightField) {
      this.nameField = nameField;
      this.heightField = heightField;
      this.weightField = weightField;
    }
    
    @Override
    public Person apply(Map<String, String> row) throws PipeException {
      try {
        return new Person(row.get(nameField), Integer.parseInt(row.get(heightField)), Integer.parseInt(row.get(weightField)));
      } catch (NumberFormatException e) {
        throw new ValidationPipeException("Illegal height or weight");
      }
    }
  }
}
