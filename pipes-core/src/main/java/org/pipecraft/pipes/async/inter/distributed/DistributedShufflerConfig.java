package org.pipecraft.pipes.async.inter.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.pipecraft.infra.math.ArithmeticUtils;
import org.pipecraft.infra.net.HostPort;
import org.pipecraft.pipes.serialization.ByteArrayCodec;

/**
 * A configuration object for {@link DistributedShufflerPipe}.
 * Works with builder design pattern.
 * 
 * @author Eyal Schneider
 */
public class DistributedShufflerConfig <T> {
  private final List<HostPort> workers;
  private final int port;
  private final Function<T, Integer> shardFunction;
  private final ByteArrayCodec<T> codec;
  
  /**
   * Private constructor
   * 
   * @param builder The builder to use for initializing this config
   */
  private DistributedShufflerConfig(Builder<T> builder) {
    this.workers = builder.workers;
    this.port = builder.port;
    this.shardFunction = builder.shardFunction;
    this.codec = builder.codec;
  }
  
  /**
   * @return a new builder
   */
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }
  
  /**
   * @return The list of workers (each specified by address+ports) taking part in the distributed shuffling. 
   * 1. Must include the current worker, with the same port as specified by the getPort(..) method.
   * 2. When used in distributed shuffling, the same set of workers must be used by all workers
   * 3. The order returned here is the one determining the shard-id to worker mapping; worker at index i works exclusively
   * on shard #i.
   */
  public List<HostPort> getWorkers() {
    return workers;
  }
  
  /**
   * @return The server port to use by current worker
   */
  public int getPort() {
    return port;
  }
  
  /**
   * @return The explicit item sharding function to use. For each item, returns the id of the shard responsible
   * to handle it (0 .. workers.size() - 1). The mapping between shard id and worker is done internally, but the caller can get it using
   * getWorkerShardId(..). 
   * By default, sharding is based on hashing of the item itself.
   */
  public Function<T, Integer> getShardFunc() {
    return shardFunction;
  }

  /**
   * @return The encoder/decoder to use for sending/receiving items to/from other workers
   */
  public ByteArrayCodec<T> getCodec() {
    return codec;
  }
  
  /**
   * Utility function
   * @param worker A worker. Expected to exist in the set of workers.
   * @return The shard-id this worker is responsible for (0 .. getWorkers().size() - 1), or -1 if not found.
   */
  public int getWorkerShardId(HostPort worker) {
    return workers.indexOf(worker);
  }
  
  public static class Builder <T> {
    private List<HostPort> workers;
    private int port;
    private Function<T, Integer> shardFunction;
    private Function<T, Object> shardBy;
    private ByteArrayCodec<T> codec;
    
    private Builder() {
    }

    /**
     * @param workers The set of workers (each specified by address+ports) taking part in the distributed shuffling. 
     * 1. Must include the current worker, with the same port as specified by the getPort(..) method.
     * 2. When used in distributed shuffling, the same set of workers must be used by all workers
     * @return This builder
     */
    public Builder<T> workers(Set<HostPort> workers) {
      this.workers = new ArrayList<>(workers);
      Collections.sort(this.workers); // Canonical sorting, determining the shard-id to worker index mapping
      return this;
    }
    
    /**
     * @param port The server port to use by current worker
     * @return This builder
     */
    public Builder<T> port(int port) {
      this.port = port;
      return this;
    }
    
    /**
     * @param shardFunction The explicit item sharding function to use. For each item, returns the id of the shard responsible
     * to handle it (0 .. workers.size() - 1). The mapping between shard id and worker is done internally, but the caller can get it using
     * DistributedShufflerPipe.getWorkerShardId(..). 
     * Note that this method overrides shardBy, and vice-versa.
     * By default, sharding is based on hashing of the item itself.
     * @return This builder.
     */
    public Builder<T> shardFunc(Function<T, Integer> shardFunction) {
      this.shardFunction = shardFunction;
      this.shardBy = null;
      return this;
    }

    /**
     * @param shardBy Specifies an object to shard by. For each item, returns an object, whose hash value will be
     * internally used to determine the shard the item belongs to. 
     * Note that this method overrides shardFunc, and vice-versa.
     * By default, sharding is based on hashing of the item itself.
     * @return This builder.
     */
    public Builder<T> shardBy(Function<T, Object> shardBy) {
      this.shardBy = shardBy;
      this.shardFunction = null;
      return this;
    }
    
    /**
     * @param codec The encoder/decoder to use for sending/receiving items to/from other workers
     * @return This builder
     */
    public Builder<T> codec(ByteArrayCodec<T> codec) {
      this.codec = codec;
      return this;
    }
    
    /**
     * @return A new config object based on the current settings and defaults, where applicable
     */
    public DistributedShufflerConfig<T> build() {
      // Workers are mandatory
      if (workers == null) {
        throw new IllegalArgumentException("worker set is mandatory");
      }
      
      // Port is mandatory
      if (port == 0) {
        throw new IllegalArgumentException("Port is mandatory");
      }

      // Codec is mandatory
      if (codec == null) {
        throw new IllegalArgumentException("Codec is mandatory");
      }

      if (shardFunction == null) {
        if (shardBy == null) {
          shardFunction = item -> ArithmeticUtils.getShardByHash(item, workers.size());
        } else {
          shardFunction = item -> ArithmeticUtils.getShardByHash(shardBy.apply(item), workers.size());
        }
      }
      
      return new DistributedShufflerConfig<>(this);
    }
  }
}
