package org.pipecraft.pipes.utils;

import java.util.Objects;

/**
 * Specifies a shard index and a total number of shards.
 * Usually used in distributed environments, where data shards are split among multiple workers. 
 * 
 * Immutable.
 * 
 * @author Eyal Schneider
 */
public class ShardSpecifier {
  // A shard specifier covering the complete item space
  public static final ShardSpecifier ALL = new ShardSpecifier(0, 1); 

  private final int shardIndex;
  private final int shardCount;
  
  /**
   * Constructor
   * 
   * @param shardIndex The index of the shard to process (must be between 0 and shardCount - 1)
   * @param shardCount The total number of shards to process
   */
  public ShardSpecifier(int shardIndex, int shardCount) {
    if (shardIndex >= shardCount) {
      throw new IllegalArgumentException("Shard index must be between 0 and shardCount-1, but got shardIndex=" + shardIndex + ", shardCount=" + shardCount);
    }
    if (shardIndex < 0) {
      throw new IllegalArgumentException("Shard index must be non-negative, but got shardIndex=" + shardIndex);
    }
    
    this.shardIndex = shardIndex;
    this.shardCount = shardCount;
  }

  /**
   * @return The index of the shard to process (must be between 0 and getShardCount() - 1)
   */
  public int getShardIndex() {
    return shardIndex;
  }

  /**
   * @return The total number of shards to process
   */
  public int getShardCount() {
    return shardCount;
  }

  @Override
  public String toString() {
    return shardIndex + "/" + shardCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShardSpecifier that = (ShardSpecifier) o;
    return shardIndex == that.shardIndex &&
        shardCount == that.shardCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardIndex, shardCount);
  }
}
