package org.pipecraft.pipes.utils;

import java.util.function.Consumer;

import org.pipecraft.pipes.sync.inter.ProgressPipe;

/**
 * A progress listener wrapping another progress listener, allowing filtering of progress events and changing the reported progress range.
 * 
 * Useful as a parameter to the {@link ProgressPipe}.
 * 
 * @author Eyal Schneider
 */
public class ProgressTransformer implements Consumer<Integer> {
  private final int from;
  private final int to;
  private final int step;
  
  private final Consumer<Integer> listener;
  private int lastPct = -1;
  
  /**
   * Constructor
   * 
   * @param listener The listener to wrap
   * @param from The initial progress to report to the underlying listener
   * @param to The final progress to report to the underlying listener
   * @param step The step size. Progress will only be reported once it completes multiples of this step size (After the transformation)
   */
  public ProgressTransformer(Consumer<Integer> listener, int from, int to, int step) {
    this.listener = listener;
    this.from = from;
    this.to = to;
    this.step = step;
  }

  /**
   * Constructor
   *
   * @param listener The listener to wrap
   * @param from The initial progress to report to the underlying listener
   * @param to The final progress to report to the underlying listener
   */
  public ProgressTransformer(Consumer<Integer> listener, int from, int to) {
    this(listener, from, to, 1);
  }

  /**
   * Constructor
   *
   * @param listener The listener to wrap
   * @param step The step size. Progress will only be reported once it completes multiples of this step size
   */
  public ProgressTransformer(Consumer<Integer> listener, int step) {
    this(listener, 0, 100, step);
  }

  @Override
  public void accept(Integer pct) {
    int adaptedPct = (int) (from + (pct / 100.0) * (to - from));
    if (adaptedPct == from && lastPct == -1|| adaptedPct == to && lastPct != 100 || adaptedPct - lastPct >= step) {
      listener.accept(adaptedPct);
      lastPct = adaptedPct;
    }
  }
}
