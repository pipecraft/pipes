package org.pipecraft.pipes.utils;

/**
 * A wrapper on queue items.
 * To be used with {@link org.pipecraft.pipes.sync.source.QueueReaderPipe} and {@link org.pipecraft.pipes.terminal.QueueWriterPipe}.
 *
 * @param <T> The wrapped item type
 *
 * @author Eyal Schneider
 */
public class QueueItem<T> {
  private final static QueueItem<?> END = new QueueItem<>();
  private final T item;
  private final Throwable throwable;

  /**
   * Constructor
   *
   * @param item The item to wrap. Null value serves as an end of data marker (successful if and only if throwable parameter is also null)
   * @param throwable An error indicating abnormal end of data. Null when the queue item doesn't indicate an error.
   */
  private QueueItem(T item, Throwable throwable) {
    this.item = item;
    this.throwable = throwable;
  }

  /**
   * Constructor
   *
   * Creates a queue item wrapping an actual item.
   * @param item The item to wrap. Non null.
   */
  private QueueItem(T item) {
    this(item, null);
  }

  /**
   * Constructor
   *
   * Creates a queue item indicating an abnormal end of data, with error.
   * @param throwable An error indicating abnormal end of data. Non null.
   */
  private QueueItem(Throwable throwable) {
    this(null, throwable);
  }

  /**
   * Constructor
   *
   * Creates a queue item indicating a successful end of data.
   */
  private QueueItem() {
    this(null, null);
  }

  /**
   * @param item The item to wrap
   * @return The queue item wrapping the given value
   */
  public static <T> QueueItem<T> of(T item) {
    return new QueueItem<>(item);
  }

  /**
   * @param e The error to wrap
   * @return A queue item indicating an abnormal termination with the given error
   */
  public static <T> QueueItem<T> error(Throwable e) {
    return new QueueItem<>(e);
  }

  /**
   * @return a queue item indicating successful end of data
   */
  @SuppressWarnings("unchecked")
  public static <T> QueueItem<T> end() {
    return (QueueItem<T>) END;
  }


  /**
   * @return The item to wrap. Null value serves as an end of data marker (successful if and only if throwable parameter is also null)
   */
  public T getItem() {
    return item;
  }

  /**
   * @return An error indicating abnormal end of data. Null when the queue item doesn't indicate an error.
   */
  public Throwable getThrowable() {
    return throwable;
  }

  /**
   * @return true if and only if the queue item indicates end of data (either successful or with error)
   */
  public boolean isEndOfData() {
    return item == null;
  }

  /**
   * @return true if and only if the queue item indicates a successful end of data
   */
  public boolean isSuccessfulEndOfData() {
    return this == END;
  }
}
