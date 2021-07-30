package org.pipecraft.pipes.sync.inter.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An item returned by the {@link SortedJoinPipe}.
 * Contains the key the join is based on, the left matches and the right matches.
 * 
 * @param <K> The key type
 * @param <L> The type of 'left' values
 * @param <R> The type of 'right' values
 *
 * @author Eyal Schneider
 */
public class JoinRecord <K, L, R> {
  private final K key;
  private final List<L> left;
  private final Map<Integer, List<R>> right;
  private int itemsCount;

  /**
   * Package protected constructor
   * 
   * @param key The key value
   * @param left The list of matching items from the left pipe
   * @param right The mapping between right pipe index (starting from 0) and the list of matching items. 
   * The pipe index is determined by it's position among right side pipes in the join input.
   * Note that the map iteration is ordered by the pipe index value.
   * Pipes with no matches should not be in the map, so lists are guaranteed to be non-empty.
   */
  JoinRecord(K key, List<L> left, Map<Integer, List<R>> right) {
    this.key = key;
    this.left = left;
    itemsCount += left.size();
    this.right = new TreeMap<>(right);
    right.values().forEach(l -> itemsCount += l.size());
  }

  /**
   * Package protected constructor
   * 
   * @param key The key value
   */
  JoinRecord(K key) {
    this.key = key;
    this.left = new ArrayList<>();
    this.right = new TreeMap<>(); // TreeMap turns out to be faster than HashMap for very few Integer keys, as in the usual case.
  }

  /**
   * @return The key value
   */
  public K getKey() {
    return key;
  }

  /**
   * Adds a left match
   * @param item The item found on left side of the join
   */
  void addLeft(L item) {
    left.add(item);
    itemsCount++;
  }
  
  /**
   * @return The list of matching items from the left pipe
   */
  public List<L> getLeft() {
    return left;
  }

  /**
   * Assumes that there's at most one matching item on left side.
   * @return The matching item from the left pipe, or null if none.
   */
  public L getSingleLeft() {
    return left.size() > 0 ? left.get(0) : null;
  }

  /**
   * Adds a right match
   * @param id The id of the right pipe
   * @param item The item found on left side of the join
   */
  void addRight(int id, R item) {
    right.computeIfAbsent(id, ArrayList::new).add(item);
    itemsCount++;
  }

  /**
   * @return Total number of items matched (from left and right sides)
   */
  public int getItemsCount() {
    return itemsCount;
  }
  
  /**
   * @return The mapping between right pipe index (starting from 0) and the list of matching items. 
   * The pipe index is determined by it's position among right side pipes in the join input.
   * Note that the map iteration is ordered by the pipe index value.
   * Pipes with no matches should not be in the map, so lists are guaranteed to be non-empty.
   */
  public Map<Integer, List<R>> getRight() {
    return right;
  }

  /**
   * Assumes that there's only one right pipe and at most one matching item in it.
   * @return The matching item from the right pipe, or null if none.
   */
  public R getSingleRight() {
    List<R> list = right.get(0);
    return list == null ? null : list.get(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinRecord<?, ?, ?> that = (JoinRecord<?, ?, ?>) o;
    return Objects.equals(key, that.key) &&
        Objects.equals(left, that.left) &&
        Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, left, right);
  }

  @Override
  public String toString() {
    return "JoinRecord [key=" + key + ", left=" + left + ", right=" + right + "]";
  }
}
