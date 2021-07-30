package org.pipecraft.pipes.async.inter.distributed;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal class to hold the num of sent bytes for address.
 *
 * Thread safe (and must be in order to be used in netty AttributeMap).
 *
 * @author Zacharya Haitin
 */
class ConnectionCounter {
  private final AtomicLong value;
  private final String address;

  /**
   * Ctor
   *
   * @param addr The address of the client.
   */
  public ConnectionCounter(InetSocketAddress addr) {
    this.address = addr.toString();
    this.value = new AtomicLong(0);
  }

  /**
   * @return The current value of sent bytes.
   */
  public long getValue() {
    return value.longValue();
  }

  /**
   * @return The client address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Adding delta to the num of sent bytes.
   *
   * @param delta num of bytes to add.
   */
  public void add(long delta) {
    this.value.addAndGet(delta);
  }
}
