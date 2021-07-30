package org.pipecraft.pipes.utils.multi;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.Storage;
import org.pipecraft.pipes.serialization.DecoderFactory;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.utils.PipeReaderSupplier;
import org.pipecraft.pipes.utils.ShardSpecifier;

/**
 * A builder + configuration for pipes reading multiple remote files from storage.
 * Suitable for sync/async pipes. 
 * Supports:
 * - Filtering files
 * - Automatic file sharding (either by file count or file volume)
 * - Reading from multiple remote paths, recursively or not
 * - Setting parallelism level
 * - Choosing whether to stream data or to download and then read locally
 * - Defining the file reading order (sync pipes only)
 * 
 * @param <T> The required data type of the items to read
 * @param <B> The file metadata type used by the storage implementation
 * 
 * @author Eyal Schneider
 */
public class StorageMultiFileReaderConfig<T, B> {
  private final Predicate<B> fileFilter;
  private final ShardSpecifier shardSpecifier;
  private final boolean balancedSharding;
  private final Bucket<B> bucket;
  private final Collection<String> paths;
  private final boolean recursivePaths; 
  private final PipeReaderSupplier<T, B> pipeSupplier;
  private final int threadNum;
  private final boolean downloadFirst;
  private final File tmpFolder;
  private final Comparator<B> fileOrder;
  
  // Private ctor
  private StorageMultiFileReaderConfig(Builder<T, B> builder) {
    this.fileFilter = builder.fileFilter;
    this.shardSpecifier = builder.shardSpecifier;
    this.balancedSharding = builder.isBalanced;
    this.bucket = builder.bucket;
    this.paths = builder.paths;
    this.recursivePaths = builder.recursivePaths;
    this.pipeSupplier = builder.pipeSupplier;
    this.threadNum = builder.threadNum;
    this.downloadFirst = builder.downloadFirst;
    this.tmpFolder = builder.tmpFolder;
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
  public static <T,B> Builder<T, B> builder(PipeReaderSupplier<T, B> supplier) {
    return new Builder<>(supplier);
  }

  /**
   * Creates a builder set up with the given file data decoder and additional defaults.
   * 
   * @param decoderFactory Defines how items should be decoded from files.
   * As an alternative when more control is needed, a {@link PipeReaderSupplier} can be passed instead.
   * @param readOptions Allows defining decompression and read buffer size to apply before decoding.
   */
  public static <T,B> Builder<T, B> builder(DecoderFactory<T> decoderFactory, FileReadOptions readOptions) {
    return new Builder<>(decoderFactory, readOptions);
  }

  /**
   * Creates a builder set up with the given file data decoder and additional defaults.
   * 
   * @param decoderFactory Defines how items should be decoded from files.
   * As an alternative when more control is needed, a {@link PipeReaderSupplier} can be passed instead.
   */
  public static <T,B> Builder<T, B> builder(DecoderFactory<T> decoderFactory) {
    return builder(decoderFactory, new FileReadOptions());
  }

  /**
   * @return The file predicate resulting from ANDing all the specified filters. 
   * Given a file metadata object, determines whether the file should be read. 
   * By default, the predicate accepts all files.
   */  
  public Predicate<B> getFileFilter() {
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
   * This option has some caveats to be aware of:
   * 1. It consumes more memory since it stores all remote file references in memory.
   * If you are working with millions of remote files, this may require careful memory settings.
   * 2. When using in a distributed system, it is the responsibility of the user to
   * guarantee that no file is added/changed once the workers start. Failing to do so will
   * result in severe silent problems such as files handled by multiple instances, or files not handled at all.
   */
  public boolean isBalancedSharding() {
    return balancedSharding;
  }

  /**
   * @return The bucket containing the files to read
   */
  public Bucket<B> getBucket() {
    return bucket;
  }

  /**
  * @return The set of paths of folders to read files from.
  * Should be relative to the bucket.
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
   * The supplier receives the file's input stream plus the file metadata, and builds
   * a pipe reading items from it. As a simple alternative, a {@link DecoderFactory} can be passed
   * as a file handler.
   */
  public PipeReaderSupplier<T, B> getPipeSupplier() {
    return pipeSupplier;
  }

  /**
   * @return The number of threads to use for reading files.
   * Refers to file download/streaming parallelism, and when used by an async pipe
   * and the download flag is set, refers also to the number of threads used to 
   * read the local files after they are all downloaded.
   * 
   * By default the thread count is set to the number of cores in the machine. 
   */
  public int getThreadNum() {
    return threadNum;
  }

  /**
   * @return When true, indicates that instead of streaming (the default behavior), we first download
   * all files (in an efficient manner), and then read them locally.
   * 
   * This approach can become relevant when any of the following applies:
   * 1) There are very few files to fetch
   * 2) The files differ significantly in their sizes
   * 3) Local disk is SSD
   */
  public boolean isDownloadFirst() {
    return downloadFirst;
  }

  /**
   * @return The temp folder to use when download flag is turned on.
   * Null when download flag is turned off.
   */
  public File getTmpFolder() {
    return tmpFolder;
  }

  /**
   * @return The order by which files should be read.
   * The order is defined as a comparator on file metadata objects.
   * This only applies for sync readers. 
   * By default order is lexicographic on the full path.
   */
  public Comparator<B> getFileOrder() {
    return fileOrder;
  }

  public static class Builder <T,B> {
    private Predicate<B> fileFilter = f -> true;
    private final Collection<Pattern> regexFilters = new ArrayList<>(); // only aggregated and ANDed with overall filter when building
    private final Collection<Predicate<String>> pathFilters = new ArrayList<>(); // only aggregated and ANDed with overall filter when building
    private ShardSpecifier shardSpecifier;
    private boolean isBalanced = false;
    private Bucket<B> bucket;
    private Collection<String> paths;
    private boolean recursivePaths = false; 
    private final PipeReaderSupplier<T, B> pipeSupplier;
    private int threadNum = Runtime.getRuntime().availableProcessors();
    private boolean downloadFirst = false;
    private File tmpFolder;
    private Comparator<B> fileOrder;

    /**
     * Constructor
     * 
     * @param supplier Produces a pipe ready to read items from a given file.
     * Setting a supplier as a file handler provides maximal control for the caller.
     * The supplier receives the file's input stream plus the file metadata, and builds
     * a pipe reading items from it. As a simple alternative, a {@link DecoderFactory} can be passed
     * as a file handler.
     */
    private Builder(PipeReaderSupplier<T, B> supplier) {
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
     * Given a file metadata object, determines whether the file should be read. 
     * By default, the filter accepts all files.
     * @return this builder
     */
    public Builder<T,B> andFilter(Predicate<B> fileFilter) {
      this.fileFilter = this.fileFilter.and(fileFilter);
      return this;
    }

    /**
     * @param filePathFilter The file predicate to AND with existing ones. 
     * Given a file path (relative to bucket), determines whether the file should be read. 
     * By default, the filter accepts all files.
     * @return this builder
     */
    public Builder<T, B> andPathFilter(Predicate<String> filePathFilter) {
      pathFilters.add(filePathFilter);
      return this;
    }

    /**
     * @param fileRegex A regex to AND with existing file filters.
     * The regex is applied on a path relative to the bucket.
     * @return this builder
     * @throws PatternSyntaxException In case the regex is illegal
     */
    public Builder<T, B> andFilter(String fileRegex) {
      Pattern pattern = Pattern.compile(fileRegex);
      regexFilters.add(pattern);
      return this;
    }

    /**
     * @param shardSpecifier Identifies the shard to read.
     * When using this method, all files passing the filters are automatically assigned a shard.
     * Sharding is based on hashing of their paths when balancing is turned off, and based on
     * file sizes when balancing is turned on.
     * @param isBalanced Indicates whether the sharding should be based on file sizes, in order to achieve
     * a semi-balanced partition of the data into shards.
     * This option has some caveats to be aware of:
     * 1. It consumes more memory since it stores all remote file references in memory.
     * If you are working with millions of remote files, this may require careful memory settings.
     * 2. When using in a distributed system, it is the responsibility of the user to
     * guarantee that no file is added/changed once the workers start. Failing to do so will
     * result in severe silent problems such as files handled by multiple instances, or files not handled at all.
     * @return this builder
     */
    public Builder<T, B> shard(ShardSpecifier shardSpecifier, boolean isBalanced) {
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
    public Builder<T, B> shard(ShardSpecifier shardSpecifier) {
      return shard(shardSpecifier, false);
    }

    /**
     * @param storage The storage implementation to use
     * @param bucketName The name of the bucket containing the files to read
     * @return this builder
     */
    public Builder<T, B> bucket(Storage<?, B> storage, String bucketName) {
      this.bucket = storage.getBucket(bucketName);
      return this;
    }

    /**
     * @param bucket The bucket containing the files to read
     * @return this builder
     */
    public Builder<T, B> bucket(Bucket<B> bucket) {
      this.bucket = bucket;
      return this;
    }

    /**
     * @param paths The set of folder paths to read files from.
     * Paths should be relative to the bucket.
     * @param isRecursive Indicates whether files should be fetched from the paths recursively or not.
     * @return this builder
     */
    public Builder<T, B> paths(Collection<String> paths, boolean isRecursive) {
      this.paths = paths;
      this.recursivePaths = isRecursive;
      return this;
    }

    /**
     * @param paths The set of folder paths to read files from, in a non-recursive manner.
     * Paths should be relative to the bucket.
     * @return this builder
     */
    public Builder<T, B> paths(Collection<String> paths) {
      return paths(paths, false);
    }

    /**
     * @param paths The set of folder paths to read files from, in a non-recursive manner.
     * Paths should be relative to the bucket.
     * @return this builder
     */
    public Builder<T, B> paths(String ... paths) {
      return paths(Arrays.asList(paths), false);
    }

    /**
     * @param path The folder path to read files from.
     * Path should be relative to the bucket.
     * @param isRecursive Indicates whether files should be fetched from the path recursively or not.
     * @return this builder
     */
    public Builder<T, B> paths(String path, boolean isRecursive) {
      return paths(Collections.singletonList(path), isRecursive);
    }

    /**
     * @param path The folder path to read files from, in a non-recursive manner.
     * Path should be relative to the bucket.
     * @return this builder
     */
    public Builder<T, B> paths(String path) {
      return paths(path, false);
    }

    /**
     * @param threadNum The number of threads to use for reading files.
     * Refers to file download/streaming parallelism, and when used by an async pipe
     * and the download flag is set, refers also to the number of threads used to 
     * read the local files after they are all downloaded.
     * 
     * By default the thread count is set to the number of cores in the machine. 
     * @return this builder
     */
    public Builder<T, B> threadNum(int threadNum) {
      this.threadNum = threadNum;
      return this;
    }

    /**
     * Indicates that instead of streaming (the default behavior), we first download
     * all files (in an efficient manner), and then read them locally.
     * @param tmpFolder The temporary folder to download to.
     * All files inside this folder will be marked as temporary (meaning that they will be deteled on JVM termination),
     * but for a timely disposal of these resources, it is strongly recommended that the caller explicitly deletes the
     * folder as soon as the pipeline ends.
     * 
     * This approach can become relevant when any of the following applies:
     * 1) There are very few files to fetch
     * 2) The files differ significantly in their sizes
     * 3) Local disk is SSD
     * @return this builder
     */
    public Builder<T, B> downloadFirst(File tmpFolder) {
      this.downloadFirst = true;
      this.tmpFolder = tmpFolder;
      return this;
    }

    /**
     * Indicates that instead of streaming (the default behavior), we first download
     * all files (in an efficient manner), and then read them locally.
     * Note: When calling this method the system's default temp folder will be used, and files
     * inside it will be created as temporary (deleted on JVM exit).
     * This is usually ok, but in cases where download operations are expected to run multiple times in the same
     * JVM run, it is recommended to pass a temp folder as a parameter, so that it can be explicitly deleted by the caller
     * when the pipeline ends.
     * 
     * This approach can become relevant when any of the following applies:
     * 1) There are very few files to fetch
     * 2) The files differ significantly in their sizes
     * 3) Local disk is SSD
     * @return this builder
     */
    public Builder<T, B> downloadFirst() {
      return downloadFirst(FileUtils.getSystemDefaultTmpFolder());
    }

    /**
     * @param fileOrder Forces an order by which files should be read.
     * The order is defined as a comparator on file metadata objects.
     * This only applies for sync reading.
     * By default order is lexicographic on the full path.
     * @return this builder
     */
    public Builder<T, B> fileOrder(Comparator<B> fileOrder) {
      this.fileOrder = fileOrder;
      return this;
    }
    
    /**
     * @return A new multi reader config based on current builder state.
     */
    public StorageMultiFileReaderConfig<T, B> build() {
      // Bucket is mandatory
      if (bucket == null) {
        throw new IllegalArgumentException("Bucket must be specified");
      }
      
      // Paths are mandatory
      if (paths == null || paths.isEmpty()) {
        throw new IllegalArgumentException("Paths must be specified");
      }
      
      // Supplier is mandatory
      if (pipeSupplier == null) {
        throw new IllegalArgumentException("File handler must be specified");
      }
      
      // Apply default ordering if needed (requires bucket object so can't be set in advance)
      if (fileOrder == null) {
        fileOrder = Comparator.comparing(m -> bucket.getPath(m));
      }
      
      // Aggregate regex filters (requires bucket object so can't be set in advance)
      for (Pattern p : regexFilters) {
        fileFilter = fileFilter.and(f -> p.matcher(bucket.getPath(f)).matches());
      }
      
      // Aggregate path filters (requires bucket object so can't be set in advance)
      for (Predicate<String> pathFilter : pathFilters) {
        fileFilter = fileFilter.and(f -> pathFilter.test(bucket.getPath(f)));
      }
      
      return new StorageMultiFileReaderConfig<>(this);
    }
  }
}
