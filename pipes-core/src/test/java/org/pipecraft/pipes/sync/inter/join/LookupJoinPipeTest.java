package org.pipecraft.pipes.sync.inter.join;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;
import org.pipecraft.pipes.terminal.TerminalPipe;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.junit.jupiter.api.Test;

/**
 * A test for the {@link LookupJoinPipe}
 * 
 * @author Eyal Schneider
 */
public class LookupJoinPipeTest {
  private static final Employee walter = new Employee(1, "Walter");
  private static final Employee donny = new Employee(2, "Donny");
  private static final Employee dude = new Employee(3, "Dude");
  private static final Employee jeff = new Employee(4, "Jeff");
  private static final Employee maude = new Employee(5, "Maude");

  @Test
  public void testJoinInner() throws Exception {
    Set<JoinRecord<Integer, Integer, Employee>> actual = new HashSet<>();
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(3, 1, 6, 5);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(maude, dude,walter, donny); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(dude, walter, jeff);  // 1, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            p0, FailableFunction.identity(),
            List.of(p1, p2),
            Employee::getRank,
            JoinMode.INNER
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
    ) {
      p.start();
      Set<JoinRecord<Integer, Integer, Employee>> expected = Set.of(
          new JoinRecord<>(3, List.of(3), Map.of(0, List.of(dude),1, List.of(dude))),
          new JoinRecord<>(1, List.of(1), Map.of(0, List.of(walter), 1, List.of(walter))),
          new JoinRecord<>(5, List.of(5), Map.of(0, List.of(maude))));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJoinOuter() throws Exception {
    Set<JoinRecord<Integer, Integer, Employee>> actual = new HashSet<>();
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(5, 3, 1, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(donny, maude, dude,walter); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(dude, jeff, walter);  // 1, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            p0, FailableFunction.identity(),
            List.of(p1, p2),
            Employee::getRank,
            JoinMode.OUTER
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
        ) {
      p.start();
      Set<JoinRecord<Integer, Integer, Employee>> expected = Set.of(
          new JoinRecord<>(1, List.of(1), Map.of(0, List.of(walter), 1, List.of(walter))),
          new JoinRecord<>(2, List.of(), Map.of(0, List.of(donny))),
          new JoinRecord<>(3, List.of(3), Map.of(0, List.of(dude), 1, List.of(dude))),
          new JoinRecord<>(4, List.of(), Map.of(1, List.of(jeff))),
          new JoinRecord<>(5, List.of(5), Map.of(0, List.of(maude))),
          new JoinRecord<>(6, List.of(6), Map.of()));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJoinLeft() throws Exception {
    Set<JoinRecord<Integer, Integer, Employee>> actual = new HashSet<>();
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(6, 1, 3, 5);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            p0, FailableFunction.identity(),
            List.of(p1, p2),
            Employee::getRank,
            JoinMode.LEFT
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
    ) {
      p.start();
      Set<JoinRecord<Integer, Integer, Employee>> expected = Set.of(
          new JoinRecord<>(1, List.of(1), Map.of(0, List.of(walter), 1, List.of(walter))),
          new JoinRecord<>(3, List.of(3), Map.of(0, List.of(dude), 1, List.of(dude))),
          new JoinRecord<>(5, List.of(5), Map.of(0, List.of(maude))),
          new JoinRecord<>(6, List.of(6), Map.of()));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJoinFullInner() throws Exception {
    Set<JoinRecord<Integer, Integer, Employee>> actual = new HashSet<>();
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 3, 5, 6);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, maude); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            p0, FailableFunction.identity(),
            List.of(p1, p2),
            Employee::getRank,
            JoinMode.FULL_INNER
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
    ) {
      p.start();

      Set<JoinRecord<Integer, Integer, Employee>> expected = Set.of(
          new JoinRecord<>(1, List.of(1), Map.of(0, List.of(walter), 1, List.of(walter))),
          new JoinRecord<>(3, List.of(3), Map.of(0, List.of(dude), 1, List.of(dude))));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testRepetitions() throws Exception {
    final Employee donny2 = new Employee(2, "Donny2");
    final Employee dude2 = new Employee(3, "Dude2");
    HashMultiSet<JoinRecord<Integer, Integer, Employee>> actual = new HashMultiSet<>();
    try (
        Pipe<Integer> p0 = new CollectionReaderPipe<>(3, 1, 3, 6, 5);
        Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny2, dude, maude, donny); // 1, 2, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(dude, walter, dude2, jeff);  // 1, 3, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            p0, FailableFunction.identity(),
            List.of(p1, p2),
            Employee::getRank,
            JoinMode.OUTER
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
    ) {
      p.start();

      assertEquals(1, actual.getCount(new JoinRecord<>(1, List.of(1), Map.of(0, List.of(walter), 1, List.of(walter)))));
      assertEquals(1, actual.getCount(new JoinRecord<>(2, List.of(), Map.of(0, List.of(donny2, donny)))));
      assertEquals(2, actual.getCount(new JoinRecord<>(3, List.of(3), Map.of(0, List.of(dude), 1, List.of(dude, dude2)))));
      assertEquals(1, actual.getCount(new JoinRecord<>(4, List.of(), Map.of(1, List.of(jeff)))));
      assertEquals(1, actual.getCount(new JoinRecord<>(5, List.of(5), Map.of(0, List.of(maude)))));
      assertEquals(1, actual.getCount(new JoinRecord<>(6, List.of(6), Map.of())));

      assertEquals(7, actual.size());
    }
  }
 
  @Test
  public void testEmptyLeft() throws Exception {
    Set<JoinRecord<Integer, Integer, Employee>> actual = new HashSet<>();
    try (
        Pipe<Employee> p1 = new CollectionReaderPipe<>(dude, donny, maude, walter); // 1, 2, 3, 5
        Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
        LookupJoinPipe<Integer, Integer, Employee> jp = new LookupJoinPipe<>(
            List.of(p1, p2),
            Employee::getRank
        );
        TerminalPipe p = new CollectionWriterPipe<>(jp, actual)
    ) {
      p.start();

      Set<JoinRecord<Integer, Integer, Employee>> expected = Set.of(
          new JoinRecord<>(1, List.of(), Map.of(0, List.of(walter), 1, List.of(walter))),
          new JoinRecord<>(2, List.of(), Map.of(0, List.of(donny))),
          new JoinRecord<>(3, List.of(), Map.of(0, List.of(dude), 1, List.of(dude))),
          new JoinRecord<>(4, List.of(), Map.of(1, List.of(jeff))),
          new JoinRecord<>(5, List.of(), Map.of(0, List.of(maude))));

      assertEquals(expected, actual);
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
      return (rank == other.rank);
    }

    @Override
    public String toString() {
      return "Employee{" +
          "rank=" + rank +
          ", name='" + name + '\'' +
          '}';
    }
  }
}