package org.pipecraft.infra.monitoring.sliding;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An abstract sliding window.
 * A sliding window object is notified with events as they occur, and supports querying the recently collected data.
 * The data aggregation and the query semantics are implementation dependent.  
 * 
 * @author Eyal Schneider
 *
 * @param <E> The type of an event to be reported
 * @param <R> The type of the record stored per time slot. Must be thread safe
 * @param <Q> The type of query results
 */
public abstract class SlidingWindow<E, R, Q> {
  private final R[] slots;
  private volatile int pos;
  private final ScheduledFuture<?> timePulseHandler;
  private final int slotTime;
  private final TimeUnit slotTimeUnit;
  
  /**
   * Constructor
   * 
   * @param slotsCount The number of time slots in the sliding window
   * @param slotTime The time duration represented by one slot
   * @param slotTimeUnit The time unit of slotTime
   * @param ex The executor to run the time-pulse task on.  
   */
  public SlidingWindow(int slotsCount, int slotTime, TimeUnit slotTimeUnit, ScheduledExecutorService ex) {
    R[] slotsTmp = newRecordArray(slotsCount);
    for(int i = 0; i < slotsTmp.length; i++)
      slotsTmp[i] = newRecord();
    slots = slotsTmp;
    this.slotTime = slotTime;
    this.slotTimeUnit = slotTimeUnit;
    
    timePulseHandler = ex.scheduleAtFixedRate(new TimePulseTask(), slotTime, slotTime, slotTimeUnit);    
  }

  /**
   * Reports a new event
   * @param event The event
   */
  public void newEvent(E event) {
    R rec = slots[pos];
    newEvent(event, rec);
  }

  /**
   * Reports a new event to be appended into a given time slot record 
   * @param event The event
   * @param rec The record to be updated
   */
  protected abstract void newEvent(E event, R rec);
  
  /**
   * @return A new, empty record. Called only during the construction of the sliding window.
   */
  protected abstract R newRecord();

  /**
   * @param size The required size
   * @return A new, empty record array of the given size.
   */
  protected abstract R[] newRecordArray(int size);

  /**
   * clears a given record by resetting its data. It is up to the implementation to decide how to deal with data consistency in case of a
   * concurrent query that inspects that record.
   * It is highly unlikely that an event is registered in the record during this operation.
   */
  protected abstract void clearRecord(R record);

  /**
   * @return The query result, relevant for the complete time period covered by the sliding window. 
   */
  public Q query() {
    return query(slots.length);
  }
  
  /**
   * @param lookback The number of time slots to look back on (must be between 1 and the number of slots defined in the constructor)
   * @return The query result, relevant for the time period covered by the given lookback. 
   */
  public Q query(int lookback) {
    if (lookback == slots.length) { // Optimization
      return query(slots);
    } else {
      int startPos = (pos - lookback + 1 + slots.length) % slots.length;
      R[] records = newRecordArray(lookback);
      
      for(int i = 0; i < lookback; i++) //TODO(Eyal): optimize this loop using 2 array copy instructions instead
      records[i] = slots[(startPos + i) % slots.length];
      return query(records);
    }
  }
  
  /**
   * @param records the array of records to inspect. Ordered from oldest to newest.
   * @return The query result, relevant for the time period covered by the given lookback. 
   */  
  protected abstract Q query(R[] records);

  /**
   * Terminates the sliding window.
   * Note: subsequent queries may return undefined results!
   */
  public void shutdown() {
    timePulseHandler.cancel(false);
  }

  /**
   * @return The time duration represented by a single slot
   */
  public int getSlotTime() {
    return slotTime;
  }

  /**
   * @return The time unit by which getSlotTime() is measured 
   */
  public TimeUnit getSlotTimeUnit() {
    return slotTimeUnit;
  }

  /**
   * @return The total time covered by this sliding window, in milliseconds
   */
  public long getWindowTimeSpanMs() {
    return getSlotsCount() * getSlotTimeUnit().toMillis(getSlotTime());    
  }

  /**
   * @return The number of slots in this sliding window 
   */
  public int getSlotsCount() {
    return slots.length;
  }

  /**
   * @param pos A position in the cyclic array
   * @return The record in the given position
   */
  protected R getRecord(int pos) {
    return slots[pos];
  }

  // The task that advances the position pointer  
  private class TimePulseTask implements Runnable {

    @Override
    public void run() {
      int nextPos = (pos + 1) % slots.length;
      clearRecord(getRecord(nextPos));
      pos = nextPos;
    }    
  }
}
