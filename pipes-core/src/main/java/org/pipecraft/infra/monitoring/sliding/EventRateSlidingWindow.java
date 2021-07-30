package org.pipecraft.infra.monitoring.sliding;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple sliding window used for calculating events rate in a given time frame
 * 
 * @author Eyal Schneider
 */
public class EventRateSlidingWindow extends SlidingWindow<Void, AtomicInteger, Long>{

  /**
   * Constructor 
   * 
   * @See {@link SlidingWindow}
   */
  public EventRateSlidingWindow(int slotsCount, int slotTime, TimeUnit slotTimeUnit, ScheduledExecutorService ex) {
    super(slotsCount, slotTime, slotTimeUnit, ex);
  }

  @Override
  protected void newEvent(Void event, AtomicInteger rec) {
    rec.incrementAndGet();
  }

  @Override
  protected AtomicInteger newRecord() {
    return new AtomicInteger();
  }

  @Override
  protected AtomicInteger[] newRecordArray(int size) {
    return new AtomicInteger[size];
  }

  @Override
  protected void clearRecord(AtomicInteger record) {
    record.set(0);
  }

  @Override
  protected Long query(AtomicInteger[] records) {
    long res = 0;
    for (AtomicInteger counter : records)
      res += counter.get();
    
    return res;
  }

  /**
   * @param timeUnit a time unit
   * @return The rate measured by this sliding window, in events per single time unit
   */
  public float query(TimeUnit timeUnit) {
    long res = query();
    
    long millisInUnit = timeUnit.toMillis(1);
    float unitsInWindow = getWindowTimeSpanMs() / (float)millisInUnit;
    
    return res / unitsInWindow;
  }

}
