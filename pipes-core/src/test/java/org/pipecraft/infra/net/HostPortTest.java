package org.pipecraft.infra.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class HostPortTest {

  @Test
  void singleLocalhostPortParsing() {
    List<HostPort> hostPorts = HostPort.getHostPorts("localhost:5000");

    assertEquals(1, hostPorts.size());
    assertEquals("localhost", hostPorts.get(0).getHost());
    assertEquals(5000, hostPorts.get(0).getPort());
  }

  @Test
  void singleIPPortParsing() {
    List<HostPort> hostPorts = HostPort.getHostPorts("1.231.3.42:5000");

    assertEquals(1, hostPorts.size());
    assertEquals("1.231.3.42", hostPorts.get(0).getHost());
    assertEquals(5000, hostPorts.get(0).getPort());
  }

  @Test
  void multipleIPPortParsing() {
    List<HostPort> hostPorts = HostPort.getHostPorts("1.231.3.42:5000,100.200.300.400:5001");

    assertEquals(2, hostPorts.size());

    assertEquals("1.231.3.42", hostPorts.get(0).getHost());
    assertEquals(5000, hostPorts.get(0).getPort());

    assertEquals("100.200.300.400", hostPorts.get(1).getHost());
    assertEquals(5001, hostPorts.get(1).getPort());
  }

  @Test
  void multipleMixedHostsParsing() {
    List<HostPort> hostPorts = HostPort.getHostPorts("1.231.3.42:5000,localhost:5001");

    assertEquals(2, hostPorts.size());

    assertEquals("1.231.3.42", hostPorts.get(0).getHost());
    assertEquals(5000, hostPorts.get(0).getPort());

    assertEquals("localhost", hostPorts.get(1).getHost());
    assertEquals(5001, hostPorts.get(1).getPort());
  }

  @Test
  void badPortError() {
    assertThrows(IllegalArgumentException.class, () -> HostPort.getHostPorts("host:badPort"));
  }
}