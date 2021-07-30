package org.pipecraft.infra.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link FutureCompletionHandler}
 * 
 * @author Eyal Schneider
 *
 */
public class FutureCompletionHandlerTest {
  
  @Test
  public void testSuccess() {
    List<String> reportedSuccessValues = new ArrayList<>();
    ListenableFuture<String> f = Futures.immediateFuture("Success!");
    FutureCompletionHandler.listenToSuccess(f, reportedSuccessValues::add);
    // Listener should run immediately, on the same thread
    assertEquals(Lists.newArrayList("Success!"), reportedSuccessValues);
  }
  
  @Test
  public void testError() {
    List<Throwable> reportedErrors = new ArrayList<>();
    ListenableFuture<String> f = Futures.immediateFailedFuture(new IOException());
    FutureCompletionHandler.listenToError(f, reportedErrors::add);
    // Listener should run immediately, on the same thread
    assertEquals(1, reportedErrors.size());
    assertTrue(reportedErrors.get(0) instanceof IOException);
  }

  @Test
  public void testSuccessAnotherThread() throws InterruptedException {
    List<String> reportedSuccessValues = Collections.synchronizedList(new ArrayList<>());
    ListenableFuture<String> f = Futures.immediateFuture("Success!");
    ExecutorService ex = Executors.newFixedThreadPool(1);
    try {
      FutureCompletionHandler.listenToSuccess(f, reportedSuccessValues::add, ex);
      assertEventuallyTrue(TimeUnit.MILLISECONDS, 15_000, 50, () -> reportedSuccessValues.size() > 0);
      assertEquals(Lists.newArrayList("Success!"), reportedSuccessValues);
    } finally {
      ex.shutdownNow();
    }
  }

  private static void assertEventuallyTrue(TimeUnit unit, long timeout, long interTestInterval, Supplier<Boolean> conditionTester) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long deadline = startTime + TimeUnit.MILLISECONDS.convert(timeout, unit);
    while (deadline > System.currentTimeMillis()) {
      if (conditionTester.get()) {
        return;
      }
      Thread.sleep(interTestInterval);
    }
    throw new AssertionError("Condition didn't become true during the given timeout interval");
  }


}
