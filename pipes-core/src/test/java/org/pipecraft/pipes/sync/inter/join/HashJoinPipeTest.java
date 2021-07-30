package org.pipecraft.pipes.sync.inter.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.serialization.DecoderFactory;
import org.pipecraft.pipes.serialization.DelegatingCodecFactory;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.serialization.TxtDecoderFactory;
import org.pipecraft.pipes.serialization.TxtEncoderFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.SeqGenPipe;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test for the {@link HashJoinPipe}, which joins multiple pipes based on some key
 *
 * @author Eyal Schneider
 */
public class HashJoinPipeTest {
  private static final EncoderFactory<Employee> EMPLOYEE_ENCODER = new TxtEncoderFactory<>(
      Employee::toCSV);
  private static final DecoderFactory<Employee> EMPLOYEE_DECODER = new TxtDecoderFactory<>(
      Employee::fromCSV);
  private static final CodecFactory<Employee> EMPLOYEE_CODEC = new DelegatingCodecFactory<>(EMPLOYEE_ENCODER, EMPLOYEE_DECODER);
  private static final CodecFactory<Integer> RANK_CODEC = new DelegatingCodecFactory<>(new TxtEncoderFactory<>(), new TxtDecoderFactory<>(Integer::parseInt));

  @Test
  public void testJoinInner() throws Exception {
    final Employee walter = new Employee(1, "Walter");
    final Employee donny = new Employee(2, "Donny");
    final Employee dude = new Employee(3, "Dude");
    final Employee jeff = new Employee(4, "Jeff");
    final Employee maude = new Employee(5, "Maude");


    try (
            Pipe<Integer> p0 = new CollectionReaderPipe<>(3, 5, 1, 6);
            Pipe<Employee> p1 = new CollectionReaderPipe<>(donny, dude, walter, maude); // 2, 3, 1, 5
            Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    p0, FailableFunction.identity(),
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    JoinMode.INNER,
                    2,
                    RANK_CODEC,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), next);
        } else if (next.getKey() == 3) {
          expectedRight.put(0, Lists.newArrayList(dude));
          expectedRight.put(1, Lists.newArrayList(dude));
          assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), next);
        } else if (next.getKey() == 5) {
          expectedRight.put(0, Lists.newArrayList(maude));
          assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), next);
        }
      }
      assertEquals(3, count);
      assertEquals(Sets.newHashSet(1, 3, 5), matchedKeys);
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
            Pipe<Integer> p0 = new CollectionReaderPipe<>(6, 1, 3, 5);
            Pipe<Employee> p1 = new CollectionReaderPipe<>(donny, dude, walter, maude); // 2, 3, 1, 5
            Pipe<Employee> p2 = new CollectionReaderPipe<>(walter, dude, jeff);  // 1, 3, 4
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    p0, FailableFunction.identity(),
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    JoinMode.OUTER,
                    2,
                    RANK_CODEC,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), next);
        } else if (next.getKey() == 2) {
          expectedRight.put(0, Lists.newArrayList(donny));
          assertEquals(new JoinRecord<>(2, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 3) {
          expectedRight.put(0, Lists.newArrayList(dude));
          expectedRight.put(1, Lists.newArrayList(dude));
          assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), next);
        } else if (next.getKey() == 4) {
          expectedRight.put(1, Lists.newArrayList(jeff));
          assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 5) {
          expectedRight.put(0, Lists.newArrayList(maude));
          assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), next);
        } else if (next.getKey() == 6) {
          assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), next);
        }
      }
      assertEquals(count, 6);
      assertEquals(Sets.newHashSet(1, 2, 3, 4, 5, 6), matchedKeys);
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
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    p0, FailableFunction.identity(),
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    JoinMode.LEFT,
                    2,
                    RANK_CODEC,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), next);
        } else if (next.getKey() == 3) {
          expectedRight.put(0, Lists.newArrayList(dude));
          expectedRight.put(1, Lists.newArrayList(dude));
          assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), next);
        } else if (next.getKey() == 5) {
          expectedRight.clear();
          expectedRight.put(0, Lists.newArrayList(maude));
          assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), next);
        }
        if (next.getKey() == 6) {
          assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), next);
        }
      }
      assertEquals(4, count);
      assertEquals(Sets.newHashSet(1, 3, 5, 6), matchedKeys);
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
            Pipe<Employee> p2 = new CollectionReaderPipe<>(jeff, walter, dude);  // 4, 1, 3
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    p0, FailableFunction.identity(),
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    JoinMode.FULL_INNER,
                    2,
                    RANK_CODEC,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), next);
        } else if (next.getKey() == 3) {
          expectedRight.put(0, Lists.newArrayList(dude));
          expectedRight.put(1, Lists.newArrayList(dude));
          assertEquals(new JoinRecord<>(3, Lists.newArrayList(3), expectedRight), next);
        }
      }
      assertEquals(2, count);
      assertEquals(Sets.newHashSet(1, 3), matchedKeys);
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
            Pipe<Integer> p0 = new CollectionReaderPipe<>(1, 6, 3, 5, 3);
            Pipe<Employee> p1 = new CollectionReaderPipe<>(walter, donny, dude, donny2, maude); // 1, 2, 3, 2, 5
            Pipe<Employee> p2 = new CollectionReaderPipe<>(jeff, walter, dude, dude2);  // 4, 1, 3, 3
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    p0, FailableFunction.identity(),
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    JoinMode.OUTER,
                    2,
                    RANK_CODEC,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(1), expectedRight), next);
        } else if (next.getKey() == 2) {
          assertEquals(Sets.newHashSet(donny, donny2), new HashSet<>(next.getRight().get(0)));
          assertEquals(1, next.getRight().size());
          assertEquals(0, next.getLeft().size());
        } else if (next.getKey() == 3) {
          assertEquals(Collections.singletonList(dude), next.getRight().get(0));
          assertEquals(Sets.newHashSet(dude, dude2), new HashSet<>(next.getRight().get(1)));
          assertEquals(2, next.getRight().size());
          assertEquals(Lists.newArrayList(3, 3), next.getLeft());
        } else if (next.getKey() == 4) {
          expectedRight.put(1, Lists.newArrayList(jeff));
          assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 5) {
          expectedRight.put(0, Lists.newArrayList(maude));
          assertEquals(new JoinRecord<>(5, Lists.newArrayList(5), expectedRight), next);
        } else if (next.getKey() == 6) {
          assertEquals(new JoinRecord<>(6, Lists.newArrayList(6), expectedRight), next);
        }
      }
      assertEquals(6, count);
      assertEquals(Sets.newHashSet(1, 2, 3, 4, 5, 6), matchedKeys);
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
            HashJoinPipe<Integer, Integer, Employee> p = new HashJoinPipe<>(
                    Lists.newArrayList(p1, p2),
                    Employee::getRank,
                    2,
                    EMPLOYEE_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Integer, Employee> next;
      int count = 0;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        count++;
        matchedKeys.add(next.getKey());
        Map<Integer, List<Employee>> expectedRight = new HashMap<>();
        if (next.getKey() == 1) {
          expectedRight.put(0, Lists.newArrayList(walter));
          expectedRight.put(1, Lists.newArrayList(walter));
          assertEquals(new JoinRecord<>(1, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 2) {
          expectedRight.put(0, Lists.newArrayList(donny));
          assertEquals(new JoinRecord<>(2, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 3) {
          expectedRight.put(0, Lists.newArrayList(dude));
          expectedRight.put(1, Lists.newArrayList(dude));
          assertEquals(new JoinRecord<>(3, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 4) {
          expectedRight.put(1, Lists.newArrayList(jeff));
          assertEquals(new JoinRecord<>(4, Lists.newArrayList(), expectedRight), next);
        } else if (next.getKey() == 5) {
          expectedRight.put(0, Lists.newArrayList(maude));
          assertEquals(new JoinRecord<>(5, Lists.newArrayList(), expectedRight), next);
        }
      }
      assertEquals(5, count);
      assertEquals(Sets.newHashSet(1, 2, 3, 4, 5), matchedKeys);
    }
  }

  @Test
  public void testLarge() throws Exception {
    List<Employee> employees = new ArrayList<>();
    for (int i = 0; i < 1_000_001; i++) { // 0..1,000,000
      employees.add(new Employee(i, String.valueOf(i)));
    }
    Random rnd = new Random(1234);
    Collections.shuffle(employees, rnd);

    try (
            Pipe<Employee> employeesP = new CollectionReaderPipe<>(employees);
            Pipe<Integer> idsP = new SeqGenPipe<>(i -> (int) (1_100_000 - i), 200_000); //900,001 to 1,100,000, in descending order
            HashJoinPipe<Integer, Employee, Integer> p = new HashJoinPipe<>(
                    employeesP, Employee::getRank,
                    idsP,
                    FailableFunction.identity(),
                    JoinMode.INNER,
                    100,
                    EMPLOYEE_CODEC,
                    RANK_CODEC,
                    FileUtils.getSystemDefaultTmpFolder()
            )) {
      p.start();
      JoinRecord<Integer, Employee, Integer> next;
      Set<Integer> matchedKeys = new HashSet<>();
      while ((next = p.next()) != null) {
        int key = next.getKey();
        assertTrue(key >= 900_001 && key <= 1_000_000);
        assertEquals(key, next.getRight().get(0).get(0));
        assertEquals(String.valueOf(key), next.getLeft().get(0).getName());
        assertTrue(matchedKeys.add(key));
      }
      assertEquals(100_000, matchedKeys.size());
    }
  }

  private static class Employee {
    private final int rank;
    private final String name;

    public Employee(int rank, String name) {
      this.rank = rank;
      this.name = name;
    }

    public int getRank() {
      return rank;
    }

    public String getName() {
      return name;
    }

    public String toCSV() {
      return name + "," + rank;
    }

    public static Employee fromCSV(String row) {
      String[] tokens = row.split(",");
      return new Employee(Integer.parseInt(tokens[1]), tokens[0]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Employee employee = (Employee) o;
      return rank == employee.rank &&
          Objects.equals(name, employee.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rank, name);
    }
  }
}