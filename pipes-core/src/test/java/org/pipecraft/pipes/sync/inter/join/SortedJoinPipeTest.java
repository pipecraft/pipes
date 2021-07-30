package org.pipecraft.pipes.sync.inter.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.Lists;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import org.pipecraft.pipes.terminal.ConsumerPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;

/**
 * A test for the {@link SortedJoinPipe}, which joins multiple pipes based on some key
 * 
 * @author Eyal Schneider
 */
public class SortedJoinPipeTest {
  @Test
  public void testJoinInner() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.INNER
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(maude));
      assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), p.next());

      assertNull(p.next());
    }
  }

  @Test
  public void testJoinOuter() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.OUTER
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(donny));
      assertEquals(new JoinRecord<>(2, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(1, Lists.newArrayList(jeff));
      assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(maude));
      assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), p.next());

      expectedRight.clear();
      assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), p.next());

      assertNull(p.next());
    }
  }

  @Test
  public void testJoinLeft() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.LEFT
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(maude));
      assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), p.next());

      expectedRight.clear();
      assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), p.next());

      assertNull(p.next());
    }
  }

  @Test
  public void testJoinFullInner() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.FULL_INNER
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), p.next());

      assertNull(p.next());
    }
  }

  @Test
  public void testRepetitions() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee donny2 = new Employee(2, "Donny2");
    final Employee dude = new Employee(3, "Dude");
    final Employee dude2 = new Employee(3, "Dude2");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, donny2, dude, maude); // 1, 2, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, dude2, jeff);  // 1, 3, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.OUTER
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(donny, donny2));
      assertEquals(new JoinRecord<>(2, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude, dude2));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(3, 3), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(1, Lists.newArrayList(jeff));
      assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(maude));
      assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), p.next());

      expectedRight.clear();
      assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), p.next());

      assertNull(p.next());
    }
  }
 
  @Test
  public void testEmptyLeft() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    try (
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare
        )) {
      p.start();
      Map<Integer, List<Employee>> expectedRight = new HashMap<>();
      expectedRight.put(0, Lists.newArrayList(walter));
      expectedRight.put(1, Lists.newArrayList(walter));
      assertEquals(new JoinRecord<>(1, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(donny));
      assertEquals(new JoinRecord<>(2, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(dude));
      expectedRight.put(1, Lists.newArrayList(dude));
      assertEquals(new JoinRecord<>(3, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(1, Lists.newArrayList(jeff));
      assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), p.next());

      expectedRight.clear();
      expectedRight.put(0, Lists.newArrayList(maude));
      assertEquals(new JoinRecord<>(5, Lists.newArrayList(), expectedRight), p.next());

      assertNull(p.next());
    }
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testEarlyExitInnerJoin() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");

    try (
        Pipe<Integer> p0 = new SeqGenPipe<>(Long::intValue); // Practically infinite pipe. After enough iterations the numeric overflow should result in out of order exception.
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        SortedJoinPipe<Integer, Integer, Employee> p = new SortedJoinPipe<>(
            p0, FailableFunction.identity(),
            Lists.newArrayList(p1, p2),
            Employee::getRank,
            Integer::compare,
            JoinMode.INNER
        );
        TerminalPipe t = new ConsumerPipe<>(p)) {
      t.start();
    }
  }

  private static class Employee{
    private final int rank;
    private final String name;
    
    public Employee(int rank, String name) {
      this.rank = rank;
      this.name = name;
    }

    public int getRank() {
      return rank;
    }

    @SuppressWarnings("unused")
    public String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + rank;
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
      Employee other = (Employee) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (rank != other.rank)
        return false;
      return true;
    }
  }
}