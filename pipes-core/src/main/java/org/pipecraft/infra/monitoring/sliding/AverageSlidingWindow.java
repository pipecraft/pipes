package org.pipecraft.infra.monitoring.sliding;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class SlotRecord {
  private final AtomicLong total = new AtomicLong();
  private final AtomicInteger totalCount = new AtomicInteger();
  
  public void inc(long value) {
    total.addAndGet(value);
    totalCount.incrementAndGet();
  }
  
  public long getTotal() {
    return total.get();
  }
  
  public int getTotalCount() {
    return totalCount.get();
  }
  
  public void clear() {
    total.set(0);
    totalCount.set(0);
  }
}

/**
 * A sliding window used for calculating the average of a stream of numbers, in a given time frame
 * 
 * @author Eyal Schneider
 */
public class AverageSlidingWindow extends SlidingWindow<Long, SlotRecord , Float>{

  /**
   * Constructor 
   * 
   * See {@link SlidingWindow}
   */
  public AverageSlidingWindow(int slotsCount, int slotTime, TimeUnit slotTimeUnit, ScheduledExecutorService ex) {
    super(slotsCount, slotTime, slotTimeUnit, ex);
  }

  @Override
  protected void newEvent(Long value, SlotRecord rec) {
    rec.inc(value);
  }

  @Override
  protected SlotRecord newRecord() {
    return new SlotRecord();
  }

  @Override
  protected SlotRecord[] newRecordArray(int size) {
    return new SlotRecord[size];
  }

  @Override
  protected void clearRecord(SlotRecord record) {
    record.clear();
  }

  @Override
  protected Float query(SlotRecord[] records) {
    long total = 0;
    long totalCount = 0;
    for (SlotRecord rec : records) {
      total += rec.getTotal();
      totalCount += rec.getTotalCount();
    }
    
    if (totalCount == 0)
      return null;
    return total / (float)totalCount;
  }

  /**
   * @return The current (approximate) count of events in the sliding window
   */
  public long count() {
    long totalCount = 0;
    for (int i = 0; i < getSlotsCount(); i++) {
      SlotRecord rec = getRecord(i);
      totalCount += rec.getTotalCount();
    }
    return totalCount;
  }
}
