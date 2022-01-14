package org.pipecraft.pipes.async.inter.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.pipecraft.infra.net.HostPort;
import org.pipecraft.pipes.AsyncTester;
import org.pipecraft.pipes.async.inter.SyncToAsyncPipe;
import org.pipecraft.pipes.serialization.ByteArrayCodec;
import org.pipecraft.pipes.serialization.ByteArrayDecoder;
import org.pipecraft.pipes.serialization.ByteArrayEncoder;
import org.pipecraft.pipes.serialization.DelegatingByteArrayCodec;
import org.pipecraft.pipes.sync.inter.AsyncToSyncPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.terminal.CollectionWriterPipe;

public class DistributedShufflerPipeTest {
  private final List<Byte> DATA = Arrays.asList((byte) 'a', (byte) 'b');
  private final ByteArrayEncoder<Byte> ENCODER = (v) -> new byte[] {v};
  private final ByteArrayDecoder<Byte> DECODER = (bytes) -> (byte) bytes[0];
  private final ByteArrayCodec<Byte> CODEC = new DelegatingByteArrayCodec<>(ENCODER, DECODER);
  
  class Runner implements Runnable {
    private final int port;
    private final Set<HostPort> hosts;
    private final List<Byte> output;

    public Runner(int port, List<Integer> hosts, List<Byte> output) {
      this.port = port;
      this.hosts = new HashSet<>(hosts.size());
      this.output = output;

      hosts.forEach((p) -> this.hosts.add(new HostPort("localhost", p)));
    }

    public void run() {
      try (CollectionReaderPipe<Byte> p1 = new CollectionReaderPipe<>(DATA);
          SyncToAsyncPipe<Byte> p2 = SyncToAsyncPipe.fromPipe(p1);
          DistributedShufflerPipe<Byte> p3 =
              new DistributedShufflerPipe<>(p2, DistributedShufflerConfig.<Byte>builder()
                  .port(port)
                  .workers(hosts)
                  .codec(CODEC).build());
          AsyncToSyncPipe<Byte> p4 = new AsyncToSyncPipe<>(p3, 10);
          CollectionWriterPipe<Byte> writer = new CollectionWriterPipe<>(p4, output)) {
        writer.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static int getRandomPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  @Test
  @Timeout(15)
  public void testOutput() throws IOException, InterruptedException {
    List<Integer> hosts = Arrays.asList(getRandomPort(), getRandomPort());
    List<AsyncTester> testers = new ArrayList<>();

    List<Byte> finalOutput = Collections.synchronizedList(new ArrayList<>());
    for (int port : hosts) {
      AsyncTester tester = new AsyncTester(new Runner(port, hosts, finalOutput));

      tester.start();
      testers.add(tester);
    }

    for (AsyncTester tester : testers) {
      tester.test();
    }

    Collections.sort(finalOutput);
    // Both of the pipes reading the same input, so it's duplicated
    Assertions.assertEquals(Arrays.asList((byte) 'a', (byte) 'a', (byte) 'b', (byte) 'b'),
        finalOutput);
  }
  
  @Test
  public void testShardIdUtils() {
    List<HostPort> workers = Arrays.asList(new HostPort("host1:1000"), new HostPort("host3:3000"), new HostPort("host2:2000"));
    
    Map<HostPort, Integer> workerShardIds = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      int shardId = DistributedShufflerPipe.getWorkerShardId(workers, i);
      workerShardIds.put(workers.get(i), shardId);
    }
    // All shards should be covered
    assertEquals(Sets.newHashSet(0, 1, 2), new HashSet<>(workerShardIds.values()));
    
    // Reordering of input list should not affect worker shards
    workers = Arrays.asList(new HostPort("host3:3000"), new HostPort("host1:1000"), new HostPort("host2:2000"));
    Map<HostPort, Integer> workerShardIds2 = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      int shardId = DistributedShufflerPipe.getWorkerShardId(workers, i);
      workerShardIds2.put(workers.get(i), shardId);
    }
    
    assertEquals(workerShardIds, workerShardIds2);
  }
}
