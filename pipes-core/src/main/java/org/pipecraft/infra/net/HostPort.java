package org.pipecraft.infra.net;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A util class for representing a network location (host+port).
 * Immutable.
 *
 * @author Zacharya Haitin
 */
public class HostPort implements Comparable<HostPort> {

  private final String host;
  private final int port;

  /**
   * Constructor.
   *
   * @param host IP or network host.
   * @param port Port for the server.
   */
  public HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * CTOR.
   *
   * @param hostPort host network string in the format "HOST:PORT".
   * @throws IllegalArgumentException if the hostPost arg is not as expected
   */
  public HostPort(String hostPort) {
    try {
      String[] parts = hostPort.split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Argument should be in the format <HOST>:<PORT>, while got " + hostPort);
      }

      this.host = parts[0];
      this.port = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("The given port is not a number: "+ hostPort, e);
    }
  }

  /**
   * @param hostsArg comma separated IP:PORT of all the workers.
   * @return host+port list of all the workers in the same order of hostsArg
   */
  public static List<HostPort> getHostPorts(String hostsArg) {
    if (hostsArg.isEmpty()) {
      return Collections.emptyList();
    }

    return Arrays.stream(hostsArg.split(","))
        .map(HostPort::new)
        .collect(Collectors.toList());
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String toString() {
    return host + ":" + port;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof HostPort)) {
      return false;
    }

    HostPort others = (HostPort) o;
    return others.getPort() == getPort() && others.getHost().equals(getHost());
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + port;
    return result;
  }

  public int compareTo(HostPort o) {
    int last = host.compareTo(o.getHost());
    return last == 0 ? Integer.compare(port, o.port) : last;
  }
}
