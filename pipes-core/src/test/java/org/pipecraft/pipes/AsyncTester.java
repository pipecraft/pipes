package org.pipecraft.pipes;

import java.lang.Thread.State;

/**
 * A generic test unit to be used in a multi-threaded test cases.
 * Starts a new thread running test code (including assertions), and waits until completion. 
 * Any assertion error in the asynch test will fail the main test.
 * 
 * @author Eyal Schneider
 */
public class AsyncTester {
  private Thread thread;
  private volatile Error error;
  private volatile RuntimeException runtimeExc;

  /**
   * Constructor
   * 
   * @param runnable The body of the test unit
   */
  public AsyncTester(final Runnable runnable) {
    thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run();
        } catch (Error e) {
          error = e;
        } catch (RuntimeException e) {
          runtimeExc = e;
        }
      }
    });
  }

  /**
   * Starts the asynch execution of the test unit.
   */
  public void start() {
    thread.start();
  }

  /**
   * Blocks until the test unit completes, and throws any runtime exceptions and errors that have
   * been thrown internally. Must not be called before start().
   * 
   * @throws InterruptedException
   */
  public void test() throws InterruptedException {
    thread.join();
    if (error != null) 
      throw error;
    if (runtimeExc != null) 
      throw runtimeExc;
  }

  /**
   * @return The state of this test thread
   */
  public State getState() {
    return thread.getState();
  }
}
