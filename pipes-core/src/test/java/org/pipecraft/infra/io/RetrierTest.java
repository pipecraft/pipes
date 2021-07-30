package org.pipecraft.infra.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import net.minidev.json.JSONObject;
import org.pipecraft.infra.concurrent.FailableInterruptibleRunnable;
import org.pipecraft.infra.concurrent.FailableInterruptibleSupplier;

/**
 * Tests {@link Retrier} class
 * 
 * @author Eyal Schneider
 */
public class RetrierTest {

  @Test
  public void testSuccessFirstAttempt() throws RuntimeException, InterruptedException {
    MutableInt attemptCounter = new MutableInt();
    Retrier.runWithDefaults(()->attemptCounter.increment());
    assertEquals(1, attemptCounter.intValue());
  }

  @Test
  public void testErrorsThenSuccess() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(2);
    Retrier.run(r, 10, 1.1, 4);
    assertEquals(3, r.getRunTimes().size()); // 2 failures then success
  }

  @Test
  public void testErrorsThenSuccessAsSupplier() throws Exception {
    TestSupplierFailable r = new TestSupplierFailable(2);
    int res = Retrier.run(r, 10, 1.1, 4);
    assertEquals(3, res);
    assertEquals(3, r.getRunTimes().size()); // 2 failures then success
  }

  @Test
  public void testRepeatedError() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(10);
    assertThrows(IOException.class, () -> Retrier.run(r, 10, 1.1, 4));
    assertEquals(4, r.getRunTimes().size()); // 4 failed attempts
  }

  @Test
  public void testDelayTimes() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(3);
    Retrier.run(r, 100, 2.0, 4);
    List<Long> runTimes = r.getRunTimes();
    assertEquals(4, runTimes.size());
    
    assertEquals(1.0, (runTimes.get(1)-runTimes.get(0)) / 100.0, 0.1);
    assertEquals(1.0, (runTimes.get(2)-runTimes.get(1)) / 200.0, 0.1);
    assertEquals(1.0, (runTimes.get(3)-runTimes.get(2)) / 400.0, 0.1);
  }

  @Test
  public void testTerminalErrors() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(FileNotFoundException.class);
    assertThrows(FileNotFoundException.class, () -> Retrier.run(r, Collections.singleton(FileNotFoundException.class), 10, 2.0, 4));
    assertEquals(1, r.getRunTimes().size());
  }

  @Test
  public void testInterruptedException() throws Exception {
    TestRunnableFailableInterrupted r = new TestRunnableFailableInterrupted();
    assertThrows(InterruptedException.class, () -> Retrier.run(r, 10, 2.0, 4));
    assertEquals(1, r.getRunTimes().size());
  }

  @Test
  public void testMonitoringEventuallySuccessful() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(2);
    Retrier retrier = new Retrier(10, 1.1, 4);
    retrier.run(r);
    JSONObject metrics = retrier.getOwnMetrics();
    assertEquals(1, metrics.get("invocations"));
    assertEquals(0, metrics.get("failedInvocations"));
    assertEquals(3, metrics.get("tries"));
  }

  @Test
  public void testMonitoringFailure() throws Exception {
    TestRunnableFailable r = new TestRunnableFailable(4);
    Retrier retrier = new Retrier(10, 1.1, 4);
    try {
      retrier.run(r);
    } catch (IOException e) {
      // Ignore
    }
    JSONObject metrics = retrier.getOwnMetrics();
    assertEquals(1, metrics.get("invocations"));
    assertEquals(1, metrics.get("failedInvocations"));
    assertEquals(4, metrics.get("tries"));
  }

  private static class TestRunnableFailable implements FailableInterruptibleRunnable<IOException> {
    private final List<Long> runTimes;
    private final int failuresBeforeSuccess;
    private final Class<? extends IOException> toThrow;

    public TestRunnableFailable(int failuresBeforeSuccess) {
      this.runTimes = new ArrayList<>();
      this.failuresBeforeSuccess = failuresBeforeSuccess;
      this.toThrow = null;
    }

    public TestRunnableFailable(Class<? extends IOException> toThrow) {
      this.runTimes = new ArrayList<>();
      this.failuresBeforeSuccess = 0;
      this.toThrow = toThrow;
    }

    public List<Long> getRunTimes() {
      return runTimes;
    }

    @Override
    public void run() throws IOException {
      runTimes.add(System.currentTimeMillis());
      
      if (toThrow != null) {
        try {
          throw toThrow.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      
      if (runTimes.size() > failuresBeforeSuccess) {
        return;
      }
      throw new IOException("Dummy error");
    }
  }

  private static class TestRunnableFailableInterrupted implements FailableInterruptibleRunnable<IOException> {
    private final List<Long> runTimes = new ArrayList<>();

    public List<Long> getRunTimes() {
      return runTimes;
    }

    @Override
    public void run() throws IOException, InterruptedException {
      runTimes.add(System.currentTimeMillis());
      throw new InterruptedException("Test");
    }
  }

  // Same like TestRunnableFailable but as a failable supplier. The returned value is the number of executions.
  private static class TestSupplierFailable implements FailableInterruptibleSupplier<Integer, IOException> {
    private final List<Long> runTimes;
    private final int failuresBeforeSuccess;
    private final Class<? extends IOException> toThrow;

    public TestSupplierFailable(int failuresBeforeSuccess) {
      this.runTimes = new ArrayList<>();
      this.failuresBeforeSuccess = failuresBeforeSuccess;
      this.toThrow = null;
    }

    public List<Long> getRunTimes() {
      return runTimes;
    }
    
    @Override
    public Integer get() throws IOException {
      runTimes.add(System.currentTimeMillis());
      
      if (toThrow != null) {
        try {
          throw toThrow.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      
      if (runTimes.size() > failuresBeforeSuccess) {
        return runTimes.size();
      }
      throw new IOException("Dummy error");
    }
  }

}
