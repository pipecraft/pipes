package org.pipecraft.pipes.utils.multi;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.serialization.DecoderFactory;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.utils.PipeReaderSupplier;
import org.pipecraft.pipes.utils.ShardSpecifier;

/**
 * A builder + configuration for pipes reading multiple local files.
 * Suitable for sync/async pipes. 
 * Supports:
 * - Filtering files
 * - Automatic file sharding (either by file count or file volume)
 * - Reading from multiple paths, recursively or not
 * - Setting parallelism level (async pipes only)
 * - Defining the file reading order (sync pipes only)
 * 
 * @param <T> The data type of the items to read
 * 
 * @author Eyal Schneider
 */
public class LocalMultiFileReaderConfig <T> {
  private final Predicate<File> fileFilter;
  private final ShardSpecifier shardSpecifier;
  private final boolean balancedSharding;
  private final Collection<String> paths;
  private final boolean recursivePaths; 
  private final PipeReaderSupplier<T, File> pipeSupplier;
  private final int threadNum;
  private final Comparator<File> fileOrder;
  
  // Private ctor
  private LocalMultiFileReaderConfig(Builder<T> builder) {
    this.fileFilter = builder.fileFilter;
    this.shardSpecifier = builder.shardSpecifier;
    this.balancedSharding = builder.isBalanced;
    this.paths = builder.paths;
    this.recursivePaths = builder.recursivePaths;
    this.pipeSupplier = builder.pipeSupplier;
    this.threadNum = builder.threadNum;
    this.fileOrder = builder.fileOrder;
  }

  /**
   * Creates a builder set up with the given pipe supplier and additional defaults.
   * 
   * @param supplier Produces a pipe ready to read items from a given file.
   * Setting a supplier as a file handler provides maximal control for the caller.
   * The supplier receives the file's input stream plus the file metadata, and builds
   * a pipe reading items from it. As a simple alternative, a {@link DecoderFactory} can be passed
   * as a file handler.
   * @return The new builder, initialized with the given supplier
   */
  public static <T> Builder<T> builder(PipeReaderSupplier<T, File> supplier) {
    return new Builder<>(supplier);
  }

  /**
   * Creates a builder set up with the given file data decoder and additional defaults.
   * 
   * @param decoderFactory Defines how items should be decoded from files.
   * As an alternative when more control is needed, a {@link PipeReaderSupplier} can be passed instead.
   * @param readOptions Allows defining decompression and read buffer size to apply before decoding.
   */
  public static <T> Builder<T> builder(DecoderFactory<T> decoderFactory, FileReadOptions readOptions) {
    return new Builder<>(decoderFactory, readOptions);
  }

  /**
   * Creates a builder set up with the given file data decoder and additional defaults.
   * 
   * @param decoderFactory Defines how items should be decoded from files.
   * As an alternative when more control is needed, a {@link PipeReaderSupplier} can be passed instead.
   */
  public static <T> Builder<T> builder(DecoderFactory<T> decoderFactory) {
    return builder(decoderFactory, new FileReadOptions());
  }
  
  /**
   * @return The file predicate resulting from ANDing all the specified filters. 
   * Given a file object, determines whether the file should be read. 
   * By default, the predicate accepts all files.
   */  
  public Predicate<File> getFileFilter() {
    return fileFilter;
  }

  /**
  * @return The identity of the shard to read.
  * Null when automatic sharding is off.
  * When enabled, all files passing the filter conditions are automatically assigned a shard,
  * and checked whether they match the specified shard.
  * Sharding is based on hashing of the file paths when balancing is turned off, and based on
  * file sizes when balancing is turned on.
  */
  public ShardSpecifier getShardSpecifier() {
    return shardSpecifier;
  }

  /**
   * Relevant only when automatic sharding is enabled.
   * @return true when balanced sharding is enabled. When enabled, sharding tries to balance
   * the total bytes volume in each shard. Otherwise, shards are more-less balanced by file count.
   * By default this parameter is false.
   * 
   * This option has some caveats to be aware about:
   * 1. It consumes more memory since it stores all file references in memory.
   * If you are working with millions of files, this may require careful memory settings.
   * 2. When using in a distributed system, it is the responsibility of the user to
   * guarantee that no file is added/changed once the workers start. Failing to do so will
   * result in severe silent problems such as files handled by multiple instances, or files not handled at all.
   */
  public boolean isBalancedSharding() {
    return balancedSharding;
  }

  /**
  * @return The set of paths (full local paths) of folders to read files from.
  */
  public Collection<String> getPaths() {
    return paths;
  }
  
  /**
   * @return true if and only if the files to fetch should be detected recursively under the
   * given paths.
   * By default set to false.
   */
  public boolean isRecursivePaths() {
    return recursivePaths;
  }

  /**
   * @return The pipe supplier specifying how items are extracted from files. 
   * Produces a pipe ready to read items from a given file.
   * Setting a supplier as a file handler provides maximal control for the caller.
   * The supplier receives the file's input stream plus the file object, and builds
   * a pipe reading items from it. As a simple alternative, a {@link DecoderFactory} can be passed
   * as a file handler.
   */
  public PipeReaderSupplier<T, File> getPipeSupplier() {
    return pipeSupplier;
  }

  /**
   * @return The number of threads to use for reading files when used by the async pipe.
   * For sync pipes this configuration has no effect.
   * By default, the number of machine cores is used.
   */
  public int getThreadNum() {
    return threadNum;
  }

  /**
   * @return The order by which files should be read.
   * The order is defined as a comparator on file objects.
   * This only applies for sync reading. 
   * By default order is lexicographic on the full path.
   */
  public Comparator<File> getFileOrder() {
    return fileOrder;
  }

  public static class Builder <T> {
    private Predicate<File> fileFilter = f -> true;
    private ShardSpecifier shardSpecifier;
    private boolean isBalanced = false;
    private Collection<String> paths;
    private boolean recursivePaths = false; 
    private final PipeReaderSupplier<T, File> pipeSupplier;
    private int threadNum = Runtime.getRuntime().availableProcessors();
    private Comparator<File> fileOrder = Comparator.comparing(File::getAbsolutePath);

    /**
     * Constructor
     * 
     * @param supplier Produces a pipe ready to read items from a given file.
     * Setting a supplier as a file handler provides maximal control for the caller.
     * The supplier receives the file's input stream plus the file metadata, and builds
     * a pipe reading items from it. As a simple alternative, a {@link DecoderFactory} can be passed
     * as a file handler.
     */
    private Builder(PipeReaderSupplier<T, File> supplier) {
      this.pipeSupplier = supplier;
    }

    /**
     * Constructor
     * 
     * @param decoderFactory Defines how items should be decoded from files.
     * As an alternative when more control is needed, a {@link PipeReaderSupplier} can be passed
     * as a file handler.
     * @param readOptions Allows defining decompression and read buffer size to apply before decoding.
     */
    private Builder(DecoderFactory<T> decoderFactory, FileReadOptions readOptions) {
      this.pipeSupplier = (is, b) -> new BinInputReaderPipe<>(is, decoderFactory, readOptions);
    }

    /**
     * @param fileFilter The file predicate to AND with existing ones. 
     * Given a file object, determines whether the file should be read. 
     * By default, the filter accepts all files.
     * @return this builder
     */
    public Builder<T> andFilter(Predicate<File> fileFilter) {
      this.fileFilter = this.fileFilter.and(fileFilter);
      return this;
    }

    /**
     * @param fileRegex A regex to AND with existing file filters.
     * The regex is applied on the full file path.
     * @return this builder
     * @throws PatternSyntaxException In case the regex is illegal
     */
    public Builder<T> andFilter(String fileRegex) {
      Pattern pattern = Pattern.compile(fileRegex);
      return andFilter(f -> pattern.matcher(f.getAbsolutePath()).matches());
    }

    /**
     * @param shardSpecifier Identifies the shard to read.
     * When using this method, all files passing the filters are automatically assigned a shard.
     * Sharding is based on hashing of their paths when balancing is turned off, and based on
     * file sizes when balancing is turned on.
     * @param isBalanced Indicates whether the sharding should be based on file sizes, in order to achieve
     * a semi-balanced partition of the data into shards.
     * This option has some caveats to be aware of:
     * 1. It consumes more memory since it stores all file references in memory.
     * If you are working with millions of files, this may require careful memory settings.
     * 2. When using in a distributed system, it is the responsibility of the user to
     * guarantee that no file is added/changed once the workers start. Failing to do so will
     * result in severe silent problems such as files handled by multiple instances, or files not handled at all.
     * @return this builder
     */
    public Builder<T> shard(ShardSpecifier shardSpecifier, boolean isBalanced) {
      this.shardSpecifier = shardSpecifier;
      this.isBalanced = isBalanced;
      return this;
    }

    /**
     * @param shardSpecifier Indicates that automatic data sharding is requested.
     * All files passing the filter conditions are automatically assigned a shard.
     * Sharding is based on hashing of their paths.
     * @return this builder
     */
    public Builder<T> shard(ShardSpecifier shardSpecifier) {
      return shard(shardSpecifier, false);
    }

    /**
     * @param paths The set of paths (full local paths) of folders to read files from.
     * @param isRecursive Indicates whether files should be fetched from the paths recursively or not.
     * @return this builder
     */
    public Builder<T> paths(Collection<String> paths, boolean isRecursive) {
      this.paths = paths;
      this.recursivePaths = isRecursive;
      return this;
    }

    /**
     * @param paths The set of paths (full local paths) of folders to read files from
     * @return this builder
     */
    public Builder<T> paths(Collection<String> paths) {
      return paths(paths, false);
    }

    /**
     * @param paths The set of paths (full local paths) of folders to read files from
     * @return this builder
     */
    public Builder<T> paths(String ... paths) {
      return paths(Arrays.asList(paths), false);
    }

    /**
     * @param folders The set of folders to read files from
     * @return this builder
     */
    public Builder<T> paths(File ... folders) {
      List<String> paths = Arrays.stream(folders).map(File::getAbsolutePath).collect(Collectors.toList());
      return paths(paths, false);
    }

    /**
     * @param path The folder path (full local path) to read files from
     * @param isRecursive Indicates whether files should be fetched from the path recursively or not.
     * @return this builder
     */
    public Builder<T> paths(String path, boolean isRecursive) {
      return paths(Collections.singletonList(path), isRecursive);
    }

    /**
     * @param path The folder path (full local path) to read from, in a non-recursive manner.
     * @return this builder
     */
    public Builder<T> paths(String path) {
      return paths(path, false);
    }

    /**
     * @param threadNum The number of threads to use for reading files when used by the async pipe.
     * For sync pipes this configuration has no effect.
     * By default, the number of machine cores is used.
     * @return this builder
     */
    public Builder<T> threadNum(int threadNum) {
      this.threadNum = threadNum;
      return this;
    }

    /**
     * @param fileOrder Forces an order by which files should be read.
     * The order is defined as a comparator on file objects.
     * This only applies for sync reading.
     * By default order is lexicographic on the full path.
     * @return this builder
     */
    public Builder<T> fileOrder(Comparator<File> fileOrder) {
      this.fileOrder = fileOrder;
      return this;
    }
    
    /**
     * @return A new multi reader config based on current builder state.
     */
    public LocalMultiFileReaderConfig<T> build() {
      // Paths are mandatory
      if (paths == null || paths.isEmpty()) {
        throw new IllegalArgumentException("Paths must be specified");
      }
      
      // Supplier is mandatory
      if (pipeSupplier == null) {
        throw new IllegalArgumentException("File handler must be specified");
      }
 
      return new LocalMultiFileReaderConfig<>(this);
    }
  }
}
