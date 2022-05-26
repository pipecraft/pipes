package org.pipecraft.infra.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;
import org.pipecraft.infra.io.Retrier;
import org.pipecraft.infra.io.SizedInputStream;

/**
 * A base class for a storage bucket implementations. 
 * 
 * Supports:
 * 
 * 1. Download/upload (single and multiple files)
 * 2. File listing
 * 3. File deletion (single and multiple files)
 * 4. Remote file copying (single and multiple files)
 * 5. Remote File moving
 * 6. Generating signed URLs with expiration for uploading/downloading files
 * 7. Getting file metadata
 * 8. Acquiring a lock using an atomic file write
 * 9. Remotely composing files, creating a single concatenated file
 * 10. Controlling access scope of uploaded files (public/private)
 *
 * Bucket terms:
 * - File - A remote path not ending with '/'
 * - Folder - A remote path ending with '/'
 * - Object Any remote entity (either file or folder)
 * 
 * Underlying bucket implementations usually treat all objects as files, and don't have a folder concept at all.
 * Here we want to follow the standard path conventions, and use "/" as a (virtual) folder separator.
 * While this class won't allow it through its API, it is still possible to use external tools to create 
 * "folder" entities (objects with paths ending with "/"). 
 * We highly recommend avoiding that, in order to prevent weird behaviors. 
 * 
 * Most of the Bucket operations support retries, and most are interruptible. 
 * See the documentation of each method.
 * 
 * All implementations must have the following properties:
 * 
 * 1. Partial data should never be seen: during a write, no reader should see 
 * partial data. Readers should see the previous file version, if any.
 * 2. Read-after-write consistency: a thread writing a file successfully and then trying to read it
 * should succeed reading the latest version.
 * 3. If the caller attempts to create a remote file with path ending with "/", the implementation
 * should reject the request and throw IOException. The motivation is to avoid the existence of "folder files"
 * (i.e. standard objects from the point of view of the cloud library, but following the naming convention of a folder),
 * since it usually creates inconsistent behaviors.
 * 4. Creating a remote file always creates the (virtual) folders leading to the file. 
 * When listing direct objects under some path, these virtual folders should be included.
 * The folder doesn't exist by itself, and when all folder files are deleted, the folder should disappear as well.
 * 5. Support uploading files with private access permissions
 * 
 * Optional features which implementations may keep unsupported:
 * 
 * 1. List-after-write consistency: a thread listing objects after a successful write should see the new file
 * 2. Exclusive file creation operation (lock functionality)
 * 3. Generating Signed URLs and resumable URLs for uploads/downloads
 * 4. Composing remote files
 * 5. Data upload into a remote file using an OutputStream
 * 6. Public access for uploaded files
 * 
 * @author Oren Peer, Eyal Schneider
 */
public abstract class Bucket<T> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String DONE_FILE_NAME = "_DONE";
  private static final byte[] EMPTY_BUFFER = new byte[0];
  private static final Long UNLIMITED_CONTENT_LENGTH = null;
  private static final Function<String, String> DEFAULT_FILE_NAME_RESOLVER = PathUtils::getLastPathPart;
  protected static final int DEFAULT_RETRY_INITIAL_SLEEP_SEC = 1;
  protected static final int DEFAULT_RETRY_MAX_ATTEMPTS = 4;
  protected static final double DEFAULT_RETRY_WAIT_TIME_FACTOR = 2.0;
  protected static final Retrier DEFAULT_RETRIER = new Retrier(DEFAULT_RETRY_INITIAL_SLEEP_SEC * 1000, DEFAULT_RETRY_WAIT_TIME_FACTOR, DEFAULT_RETRY_MAX_ATTEMPTS);

  private final String bucketName;

  /**
   * Constructor
   *
   * @param bucketName The bucket name
   */
  public Bucket(String bucketName) {
    this.bucketName = bucketName;
  }

  /**
   * @return The bucket name
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Uploads data into a remote file from a given {@link InputStream}
   *
   * @param key full path of remote file, relative to the bucket
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation. The stream is closed by this method.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @param allowOverride When false, if the remote file already exists the call will fail with {@link FileAlreadyExistsException}.
   * This is an optional feature. Implementations may throw {@link UnsupportedOperationException} when it's set to true.
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal here, because it follows the folder naming convention.
   * @throws FileAlreadyExistsException In case that allowOverride is on and supported, and the target file already exists
   * @throws UnsupportedOperationException In case that an exclusive write was requested but the implementation doesn't support this feature
   */
  public abstract void put(String key, InputStream input, long length, String contentType, boolean isPublic, boolean allowOverride) throws IOException;

  /**
   * Uploads data into a remote file from a given {@link InputStream}. If the file exists,
   * it's being overridden.
   *
   * @param key full path of remote file, relative to the bucket
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation. The stream is closed by this method.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void put(String key, InputStream input, long length, String contentType, boolean isPublic) throws IOException {
    put(key, input, length, contentType, isPublic, true);
  }

  /**
   * Uploads file to given path
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The local input file to read from
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void putFile(String key, File input, boolean isPublic) throws IOException {
    put(key, new FileInputStream(input), input.length(), null, isPublic);
  }

  /**
   * Writes a file in an exclusive manner, providing lock semantics.
   * This method guarantees that at most one writer will succeed creating the file.
   * The created file is empty, with private access. 
   *
   * Not all implementations support this feature, and {@link UnsupportedOperationException} may be thrown.
   *
   * @param key the lock file path (including file name), relative to the bucket
   * @return true when lock operation succeeded, false when the lock was already acquired
   * @throws IOException when locking attempt failed (not due to locking failure, which is indicated by the returned value)
   * @throws UnsupportedOperationException In case that the operation isn't supported
   */
  public boolean putLockFile(String key) throws IOException {
    try (InputStream emptyStream = new ByteArrayInputStream(EMPTY_BUFFER)){
      put(key, emptyStream, 0, "text/plain", false, false);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    }
  }

  /**
   * Writes an empty file to the given path, setting private access. If the file exists, it's being overridden.
   *
   * @param key the lock file path (including file name), relative to the bucket
   * @throws IOException In case of IO error, or if the supplied key has a form of a folder rather than a file
   */
  public void putEmptyFile(String key) throws IOException {
    try (InputStream emptyStream = new ByteArrayInputStream(EMPTY_BUFFER)) {
      put(key, emptyStream, 0, "text/plain", false, true);
    }
  }

  /**
   * Put a _DONE file in a given cloud folder.
   * _DONE files are empty files used as a convention to signal a consumer of the folder files that the data is complete, in order to avoid a scenario where the consumer reads partial data
   * while the producer is still uploading files to the folder. While single file consistency is guaranteed, there's no other
   * method except done files to guarantee that a complete folder is created and ready.
   *
   * If the done file already exists, it is being overridden.
   *
   * @param folderPath The remote folder path. Treated as a folder path anyway - the called may or may not
   * add a '/' to the path.
   * @return The full path (relative to the bucket) of the new remote _DONE file
   * @throws IOException In case of write error
   */
  public String putDoneFile(String folderPath) throws IOException {
    String key = PathUtils.buildPath(folderPath, DONE_FILE_NAME);
    putEmptyFile(key);
    return key;
  }

  /**
   * Gets an output stream for writing to a remote file. The file is assigned private access permissions.
   * Note that since this is an optional feature, implementations may throw {@link UnsupportedOperationException}
   *
   * @param key The remote target file path, relative to the bucket. May exist or not.
   * Overridden by this method if exists.
   * @return The output stream to write to. Not buffered. There is no guarantee that the file is written unless the writing completes with no errors,
   * and the stream is closed by the caller. The written data will override the existing remote file, if exists, in an atomic manner.
   * The output stream is not allowed to throw runtime exceptions for IO errors while writing - only {@link IOException}s are allowed.
   * @throws IOException in case of a write error, or if the supplied key has a form of a folder rather than a file
   * @throws UnsupportedOperationException In case that the implementation doesn't support this optional operation
   */
  public OutputStream getOutputStream(String key) throws IOException {
    return getOutputStream(key, 0);
  }

  /**
   * Gets an output stream for writing to a remote file. The file is assigned private access permissions.
   * Note that since this is an optional feature, implementations may throw {@link UnsupportedOperationException}
   *
   * @param key The remote target file path, relative to the bucket. May exist or not.
   * Overridden by this method if exists.
   * @param chunkSize The size (in bytes) of each written chunk, or 0 for using the default one.
   * @return The output stream to write to. Not buffered. There is no guarantee that the file is written unless the writing completes with no errors,
   * and the stream is closed by the caller. The written data will override the existing remote file, if exists, in an atomic manner.
   * The output stream is not allowed to throw runtime exceptions for IO errors while writing - only {@link IOException}s are allowed.
   * @throws IOException in case of a write error, or if the supplied key has a form of a folder rather than a file
   * @throws UnsupportedOperationException In case that the implementation doesn't support this optional operation
   */
  public abstract OutputStream getOutputStream(String key, int chunkSize) throws IOException;

  /**
   * Uploads file to given path and sets public read access
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The local input file to read from
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void putPublic(String key, File input) throws IOException {
    putFile(key, input, true);
  }

  /**
   * Uploads file from InputStream and sets public read access
   *
   * @param key full path of remote file, relative to the bucket
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void putPublic(String key, InputStream input, long length, String contentType) throws IOException {
    put(key, input, length, contentType, true);
  }

  /**
   * Uploads file, using an auto generated key, to given remote folder and sets public read access
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input The local input file to read from
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, with no extension.
   * @throws IOException in case of an IO error
   */
  public String putUniquePublic(String folderPath, File input) throws IOException {
    String key = generateUniqueKey(folderPath, "");
    putPublic(key, input);
    return key;
  }

  /**
   * Uploads file from InputStream to unique key under given path and sets public read access
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, with no extension.
   * @throws IOException In case of write error
   */
  public String putUniquePublic(String folderPath, InputStream input, long length, String contentType) throws IOException {
    return putUniquePublic(folderPath, input, null, length, contentType);
  }

  /**
   * Uploads a file from InputStream to a unique key under the given path and sets public read access.
   * This method allows to add a suffix (or file extension) to the generated key.
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation.
   * @param extension The suffix that should be added to the unique key. In case the value of this parameter is null,
   * no extension will be added (the whole filename will be auto generated)
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, ending with the given extension
   * (if non-null).
   * @throws IOException In case of write error
   */
  public String putUniquePublic(String folderPath, InputStream input, String extension, long length, String contentType) throws IOException {
    if (extension == null) {
      extension = "";
    }
    String key = generateUniqueKey(folderPath, extension);
    putPublic(key, input, length, contentType);
    return key;
  }

  /**
   * Uploads file to given path and sets private read access
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The local input file to read from
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void putPrivate(String key, File input) throws IOException {
    putFile(key, input, false);
  }

  /**
   * Uploads file from InputStream and sets private read access
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The input stream to read from. Buffering is not required, and is assumed
   * to be added by the implementation.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @throws IOException in case of an actual IO error, or if the given key is illegal. A path ending with '/'
   * is considered to be illegal in this context, because it follows the folder naming convention.
   */
  public void putPrivate(String key, InputStream input, long length, String contentType) throws IOException {
    put(key, input, length, contentType, false);
  }

  /**
   * Uploads file, using an auto generated key, to given remote folder and sets private read access
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input The local input file to read from
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, with no extension.
   * @throws IOException in case of an IO error
   */
  public String putUniquePrivate(String folderPath, File input) throws IOException {
    String key = generateUniqueKey(folderPath, "");
    putPrivate(key, input);
    return key;
  }

  /**
   * Uploads file from InputStream to unique key under given path and sets private read access
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation.
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, with no extension.
   * @throws IOException In case of write error
   */
  public String putUniquePrivate(String folderPath, InputStream input, long length, String contentType) throws IOException {
    return putUniquePrivate(folderPath, input, null, length, contentType);
  }

  /**
   * Uploads a file from InputStream to a unique key under the given path and sets private read access.
   * This method allows to add a suffix (or file extension) to the generated key.
   *
   * @param folderPath full path of the target folder in storage, relative to the bucket.
   * Treated as folder anyway - the caller may or may not add '/' to the path.
   * @param input {@link InputStream} source. Buffering is not required, and is assumed to be
   * added by the implementation.
   * @param extension The suffix that should be added to the unique key. In case the value of this parameter is null,
   * no extension will be added (the whole filename will be auto generated)
   * @param length size in bytes of input stream.
   * Not all implementations require this field, and it depends on the cloud library requirements.
   * Please read the specific implementation details.
   * @param contentType content mime-type (e.g. "image/jpeg"). Not mandatory, may be null.
   * @return The full path (relative to the bucket) of the generated file.
   * The file name itself is composed of a generated character sequence, ending with the given extension
   * (if non-null).
   * @throws IOException In case of write error
   */
  public String putUniquePrivate(String folderPath, InputStream input, String extension, long length, String contentType) throws IOException {
    if (extension == null) {
      extension = "";
    }
    String key = generateUniqueKey(folderPath, extension);
    putPrivate(key, input, length, contentType);
    return key;
  }

  /**
   * Uploads file to given path, with retries
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The local input File
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException in case of an upload failure, or if the supplied key has a form of a folder rather than a file
   * @throws InterruptedException in case of an interruption during retries
   */
  public void putFileInterruptibly(String key, File input, boolean isPublic, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> put(key, new FileInputStream(input), input.length(), null, isPublic),
        Collections.singleton(FileNotFoundException.class), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Uploads file to given path, with retries.
   * Uses default retry settings.
   *
   * @param key full path of remote file, relative to the bucket
   * @param input The local input File
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @throws IOException in case of an upload failure, or if the supplied key has a form of a folder rather than a file
   * @throws InterruptedException in case of an interruption during retries
   */
  public void putFileInterruptibly(String key, File input, boolean isPublic) throws IOException, InterruptedException {
    putFileInterruptibly(key, input, isPublic, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Uploads all regular files from a given folder. Performs the task in parallel, using retries on individual files.
   *
   * @param targetFolder full path of remote folder, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param inputFolder The local input folder to read all files from
   * @param parallelism The number of threads to use for the task
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException in case of an upload failure
   * @throws InterruptedException in case of an interruption during retries
   */
  public void putAllInterruptibly(String targetFolder, File inputFolder, int parallelism, boolean isPublic, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    String finalPath = normalizeFolderPath(targetFolder);

    if (!inputFolder.isDirectory()) {
      throw new FileNotFoundException("The folder " + inputFolder.getAbsolutePath() + " does not exist or isn't a folder");
    }
    List<File> files = Arrays.asList(inputFolder.listFiles(File::isFile));

    ParallelTaskProcessor.runFailable(files, parallelism, f -> {
      putFileInterruptibly(finalPath + f.getName(), f, isPublic, maxRetries, initialRetrySleepSec, waitTimeFactor);
    });
  }

  /**
   * Uploads all regular files from a given folder. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   *
   * @param targetFolder full path of remote folder, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param inputFolder The local input folder to read all files from
   * @param parallelism The number of threads to use for the task
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @throws IOException in case of an upload failure
   * @throws InterruptedException in case of an interruption during retries
   */
  public void putAllInterruptibly(String targetFolder, File inputFolder, int parallelism, boolean isPublic) throws IOException, InterruptedException {
    putAllInterruptibly(targetFolder, inputFolder, parallelism, isPublic, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * uploads a complete local folder to the cloud, recursively.
   * Performs the task in parallel, using retries on individual files.
   *
   * @param targetFolder full path of remote folder, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param inputFolder The local input folder to read all contents from, recursively.
   * Empty folders under the input folder aren't copied.
   * @param parallelism The number of threads to use for the task
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while uploading the files
   * @throws InterruptedException In case that the current thread is interrupted during the operation
   */
  public void putAllRecursiveInterruptibly(String targetFolder, File inputFolder, int parallelism, boolean isPublic, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    String finalPath = normalizeFolderPath(targetFolder);

    if (!inputFolder.isDirectory()) {
      throw new FileNotFoundException("The folder " + inputFolder.getAbsolutePath() + " does not exist or isn't a folder");
    }
    Path basePath = inputFolder.toPath();
    List<Path> files = Files.walk(basePath).filter(p -> p.toFile().isFile()).map(basePath::relativize).collect(Collectors.toList());

    ParallelTaskProcessor.runFailable(files, parallelism, f -> {
      putFileInterruptibly(finalPath + f.toFile().getPath(), basePath.resolve(f).toFile(), isPublic, maxRetries, initialRetrySleepSec, waitTimeFactor);
    });
  }

  /**
   * uploads a complete local folder to the cloud, recursively.
   * Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   *
   * @param targetFolder full path of remote folder, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param inputFolder The local input folder to read all contents from, recursively.
   * Empty folders under the input folder aren't copied.
   * @param parallelism The number of threads to use for the task
   * @param isPublic true to set public file access, false for private. Not all implementations support public, so this
   * parameter may be ignored.
   * @throws IOException In case of IO error while uploading the files
   * @throws InterruptedException In case that the current thread is interrupted during the operation
   */
  public void putAllRecursiveInterruptibly(String targetFolder, File inputFolder, int parallelism, boolean isPublic) throws IOException, InterruptedException {
    putAllRecursiveInterruptibly(targetFolder, inputFolder, parallelism, isPublic, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Downloads a file given the remote file's metadata object
   *
   * @param meta The metadata object pointing to the file to download
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @throws FileNotFoundException In case that the required object wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   */
  public abstract void get(T meta, File output) throws IOException;

  /**
   * Downloads a file given the remote file path
   *
   * @param key Remote file path, relative to the bucket
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   */
  public void get(String key, File output) throws IOException {
    T meta = getObjectMetadata(key);
    get(meta, output);
  }

  /**
   * Downloads a file from cloud storage, using sliced download.
   * The default implementation simply delegates to get(key, File), but subclasses may implement this
   * using concurrent download of different file slices as the method name indicates.
   *
   * @param key Remote file path, relative to the bucket
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @param chunkSize The size (in bytes) of each chunk read from the storage at once, or 0 for using the default one.
   * This parameter may be ignored in implementations where it's not relevant.
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to the target file.
   * This includes non-existing folder in the local file path.
   * @throws InterruptedException In case that the current thread is interrupted during the operation
   */
  public void getSliced(String key, File output, int chunkSize) throws IOException, InterruptedException {
    get(key, output);
  }

  /**
   * Downloads a file from cloud storage, using sliced download.
   * The default implementation simply delegates to getSliced(key, File, chunkSize),
   * so if subclasses don't override the latter, then the default single threaded
   * download will be used.
   *
   * @param key Remote file path, relative to the bucket
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to the target file.
   * This includes non-existing folder in the local file path.
   */
  public void getSliced(String key, File output) throws IOException {
    try {
      getSliced(key, output, 0);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(); // TODO: add InterruptedException to signature and fix existing usages
    }
  }

  /**
   * Gets an input stream for a remote file, using a given chunk size.
   * The chunk size indicates how many bytes to fetch in each request, and may be ignored by some implementations.
   *
   * @param meta The remote object metadata
   * @param chunkSize The size (in bytes) of each chunk read from the storage at once, or 0 for using the default one.
   * This parameter may be ignored in implementations where it's not relevant.
   * @return The input stream to read the requested resource from.
   * Note that this stream is a {@link SizedInputStream}, therefore it provides the data size.
   * The stream is aware of any RuntimeException thrown by the underlying cloud library,
   * and convents it to a proper IOException. The returned stream isn't buffered.
   * @throws IOException In case of a read error
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   */
  public abstract SizedInputStream getAsStream(T meta, int chunkSize) throws IOException;

  /**
   * Gets an input stream for a remote file, using the default chunk size.
   *
   * @param key The remote file path, relative to the bucket
   * @return The input stream to read the requested resource from.
   * Note that this stream is a {@link SizedInputStream}, therefore it provides the data size.
   * The stream is aware of any RuntimeException thrown by the underlying cloud library,
   * and convent it a proper IOException. The returned stream isn't buffered.
   * @throws IOException In case of a read error
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   */
  public SizedInputStream getAsStream(String key) throws IOException {
    return getAsStream(key, 0);
  }

  /**
   * Gets an input stream for a remote file, using the default chunk size.
   *
   * @param meta The remote object metadata
   * @return The input stream to read the requested resource from.
   * Note that this stream is a {@link SizedInputStream}, therefore it provides the data size.
   * The stream is aware of any RuntimeException thrown by the underlying cloud library,
   * and convent it a proper IOException. The returned stream isn't buffered.
   * @throws IOException In case of a read error
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   */
  public SizedInputStream getAsStream(T meta) throws IOException {
    return getAsStream(meta, 0);
  }

  /**
   * Gets an input stream for a remote file, using a given chunk size.
   * The chunk size indicates how many bytes to fetch in each request, and may be ignored by some implementations.
   *
   * @param key The remote file path, relative to the bucket
   * @param chunkSize The size (in bytes) of each chunk read from the storage at once, or 0 for using the default one.
   * This parameter may be ignored in implementations where it's not relevant.
   * @return The input stream to read the requested resource from.
   * Note that this stream is a {@link SizedInputStream}, therefore it provides the data size.
   * The stream is aware of any RuntimeException thrown by the underlying cloud library,
   * and convent it a proper IOException. The returned stream isn't buffered.
   * @throws IOException In case of a read error
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   */
  public SizedInputStream getAsStream(String key, int chunkSize) throws IOException {
    return getAsStream(getObjectMetadata(key), chunkSize);
  }

  /**
   * @param key Path to an existing json file, relative to the bucket
   * @param clazz The java class corresponding to the json file's structure. This serves as a deserialization spec.
   * @return An instance of the given class, populated with the data read from the json file.
   * @throws IOException In case of read error
   * @throws IllegalJsonException In case that the json is illegal and can't be deserialized properly
   */
  public <C> C getFromJson(String key, Class<C> clazz) throws IOException, IllegalJsonException {
    InputStream is = getAsStream(key);
    try {
      return OBJECT_MAPPER.readValue(is, clazz);
    } catch (JsonProcessingException e) {
      // Unfortunately this is an unchecked exception, and we don't want to rethrow it as such,
      // because it's up to the caller to decide whether this is a serious situation or not
      throw new IllegalJsonException("Illegal json in '" + getBucketName() + "/" + key + "'", e);
    }
  }

  /**
   * Downloads a file with retries
   *
   * @param key Remote file path, relative to the bucket
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void getInterruptibly(String key, File output, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    T meta = getObjectMetadata(key);
    getInterruptibly(meta, output, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Downloads a file with retries
   *
   * @param meta The metadata object pointing to the file to download
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void getInterruptibly(T meta, File output, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> get(meta, output),
        Collections.singleton(FileNotFoundException.class), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Downloads a file with retries
   * Uses default retry settings
   *
   * @param key Remote file path, relative to the bucket
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void getInterruptibly(String key, File output) throws IOException, InterruptedException {
    T meta = getObjectMetadata(key);
    getInterruptibly(meta, output);
  }

  /**
   * Downloads a file with retries
   * Uses default retry settings
   *
   * @param meta The metadata object pointing to the file to download
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file or writing to local file.
   * This includes non-existing folder in the local file path.
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void getInterruptibly(T meta, File output) throws IOException, InterruptedException {
    getInterruptibly(meta, output, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Downloads all regular files from a given folder, performing the task in parallel
   * Uses retries on individual files, using default retry settings.
   *
   * @param folderPath The source folder path, relative to the bucket.
   * Treated as a folder anyway, meaning that the caller may or may not add '/' to it.
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded  file objects
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(String folderPath, File targetFolder, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesInterruptibly(folderPath, b -> true, targetFolder, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Downloads regular files from a given remote folder.
   * Performs the task in parallel, and allows filtering the files to download by a predicate.
   * Retries are done on individual files, using default retry settings.
   *
   * @param folderPath The source folder path, relative to the bucket.
   * Treated as a folder anyway, meaning that the caller may or may not add '/' to it.
   * @param predicate A predicate on the file objects, defining which files to select for download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded  file objects
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(String folderPath, Predicate<T> predicate, File targetFolder, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    folderPath = normalizeFolderPath(folderPath);

    Iterator<T> it = listObjects(folderPath, predicate);
    List<T> remoteFileObjects = Streams.stream(it).filter(this::isFile).collect(Collectors.toList());

    return getAllRegularFilesByMetaInterruptibly(remoteFileObjects, targetFolder, DEFAULT_FILE_NAME_RESOLVER, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Downloads regular files from a given remote folder.
   * Performs the task in parallel, and allows filtering the files to download by a predicate.
   * Retries are done on individual files, using default retry settings.
   *
   * @param folderPath The source folder path, relative to the bucket.
   * Treated as a folder anyway, meaning that the caller may or may not add '/' to it.
   * @param predicate A predicate on the file objects, defining which files to select for download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded  file objects
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(String folderPath, Predicate<T> predicate, File targetFolder, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesInterruptibly(folderPath, predicate, targetFolder, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   *
   * @param folderPaths The list of paths of folders to download all direct files from
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (withoug path).
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded  file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   */
  public Set<File> getAllRegularFiles(Collection<String> folderPaths, File targetFolder, Function<String, String> fileNameResolver, int parallelism) throws IOException {
    try {
      return getAllRegularFilesInterruptibly(folderPaths, targetFolder, fileNameResolver, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Expects a fileNameResolver, which allows assigning different names to target files. This may be useful
   * for example when protecting from remote files with the same name to override each other.
   *
   * @param cloudFilePaths The list of remote paths of files to download. Relative to the bucket.
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (just name, without path).
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(Collection<String> cloudFilePaths, File targetFolder, Function<String, String> fileNameResolver, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Map<String, T> metaObjs = getObjectMetadata(cloudFilePaths,maxRetries, initialRetrySleepSec, waitTimeFactor);
    validateAllPathsExist(metaObjs);
    return getAllRegularFilesByMetaInterruptibly(metaObjs.values(), targetFolder, fileNameResolver, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * The local file names will be exactly as in their remote copy. This means that remote files from
   * different folders will override each other if they have the same name.
   *
   * @param cloudFilePaths The list of remote paths of files to download. Relative to the bucket.
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(Collection<String> cloudFilePaths, File targetFolder, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    return getAllRegularFilesInterruptibly(cloudFilePaths, targetFolder, DEFAULT_FILE_NAME_RESOLVER, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   * Expects a fileNameResolver, which allows assigning different names to target files. This may be useful
   * for example when protecting from remote files with the same name to override each other.
   *
   * @param cloudFilePaths The collection of remote paths of files to download. Relative to the bucket.
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (just name, without path).
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(Collection<String> cloudFilePaths, File targetFolder, Function<String, String> fileNameResolver, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesInterruptibly(cloudFilePaths, targetFolder, fileNameResolver, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   * The local file names will be exactly as in their remote copy. This means that remote files from
   * different folders will override each other if they have the same name.
   *
   * @param filePaths The collection of remote paths of files to download. Relative to the bucket.
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesInterruptibly(Collection<String> filePaths, File targetFolder, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesInterruptibly(filePaths, targetFolder, DEFAULT_FILE_NAME_RESOLVER, parallelism);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Expects a fileNameResolver, which allows assigning different names to target files. This may be useful
   * for example when protecting from remote files with the same name to override each other.
   *
   * @param metaObjects The collection of objects pointing to the remote bucket files to download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (just name, without path).
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesByMetaInterruptibly(Collection<T> metaObjects, File targetFolder, Function<String, String> fileNameResolver, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    targetFolder.mkdirs();
    if (!targetFolder.isDirectory()) {
      throw new IOException("Not a folder: " + targetFolder);
    }

    Set<File> res = Collections.synchronizedSet(new HashSet<>());

    ParallelTaskProcessor.runFailable(metaObjects, parallelism, f -> {
      File localFile = new File(targetFolder, fileNameResolver.apply(getPath(f)));
      getInterruptibly(f, localFile, maxRetries, initialRetrySleepSec, waitTimeFactor);
      res.add(localFile);
    });
    return res;
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * The local file names will be exactly as in their remote copy. This means that remote files from
   * different folders will override each other if they have the same name.
   *
   * @param metaObjects The collection of objects pointing to the remote files to download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesByMetaInterruptibly(Collection<T> metaObjects, File targetFolder, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    return getAllRegularFilesByMetaInterruptibly(metaObjects, targetFolder, DEFAULT_FILE_NAME_RESOLVER, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   * Expects a fileNameResolver, which allows assigning different names to target files. This may be useful
   * for example when protecting from remote files with the same name to override each other.
   *
   * @param metaObjects The collection of objects pointing to the remote files to download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (just name, without path).
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesByMetaInterruptibly(Collection<T> metaObjects, File targetFolder, Function<String, String> fileNameResolver, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesByMetaInterruptibly(metaObjects, targetFolder, fileNameResolver, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Downloads a set of regular files from different bucket locations. Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   * The local file names will be exactly as in their remote copy. This means that remote files from
   * different folders will override each other if they have the same name.
   *
   * @param metaObjects The collection of objects pointing to the remote files to download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param parallelism The number of threads to use for the task
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public Set<File> getAllRegularFilesByMetaInterruptibly(Collection<T> metaObjects, File targetFolder, int parallelism) throws IOException, InterruptedException {
    return getAllRegularFilesByMetaInterruptibly(metaObjects, targetFolder, DEFAULT_FILE_NAME_RESOLVER, parallelism);
  }

  /**
   * Copies a file between this bucket to another bucket
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toBucket The name of the target bucket (may be the current bucket name)
   * @param toKey The path of the target file, relative to the target bucket.
   * Overridden if exists.
   * @throws IOException In case of a read/write error
   */
  public abstract void copyToAnotherBucket(String fromKey, String toBucket, String toKey) throws IOException;

  /**
   * Copies a a remote file in the current bucket to a different location in the same bucket
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toKey The path of the target file, relative to this bucket
   * @throws IOException In case of a read/write error
   */
  public void copy(String fromKey, String toKey) throws IOException {
    copyToAnotherBucket(fromKey, getBucketName(), toKey);
  }

  /**
   * Copies a a remote file in the current bucket to a different location in the same bucket.
   * Uses retries.
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toKey The path of the target file, relative to this bucket
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException in case the client or the service has failed
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyInterruptibly(String fromKey, String toKey, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> copy(fromKey, toKey), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Copies a a remote file in the current bucket to a different location in the same bucket.
   * Uses retries with default retry settings.
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toKey The path of the target file, relative to this bucket
   * @throws IOException in case the client or the service has failed
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyInterruptibly(String fromKey, String toKey) throws IOException, InterruptedException {
    copyInterruptibly(fromKey, toKey, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Copies a file between this bucket to another bucket, using retries
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toBucket The name of the target bucket
   * @param toKey The path of the target file, relative to the target bucket
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException in case the client or the service has failed
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyToAnotherBucketInterruptibly(String fromKey, String toBucket, String toKey, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> copyToAnotherBucket(fromKey, toBucket, toKey), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Copies a file between this bucket to another bucket, using retries.
   * Uses default retry settings.
   *
   * @param fromKey The path of the source file, relative to this bucket
   * @param toBucket The name of the target bucket
   * @param toKey The path of the target file, relative to the target bucket
   * @throws IOException in case the client or the service has failed
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyToAnotherBucketInterruptibly(String fromKey, String toBucket, String toKey) throws IOException, InterruptedException {
    copyToAnotherBucketInterruptibly(fromKey, toBucket, toKey, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Copies all files, recursively, from one folder in this bucket to another.
   * Performs the task in parallel, using retries on individual files.
   *
   * @param srcPath source path of the folder to copy, relative to the bucket.
   * Treated as folder, meaning that the caller may or may not append '/' to the path/
   * @param dstPath destination path of the folder to copy into, relative to the bucket.
   * Treated as folder, meaning that the caller may or may not append '/' to the path/
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of error copying the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyFolderRecursiveInterruptibly(String srcPath, String dstPath, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    String srcPathNorm = normalizeFolderPath(srcPath);
    String dstPathNorm = normalizeFolderPath(dstPath);
    Iterator<T> it = listFilesRecursive(srcPathNorm);
    List<Pair<String, String>> srcToDstPaths = Streams.stream(it)
        .map(this::getPath)
        .map(srcFilePath -> Pair.of(srcFilePath, srcFilePath.replace(srcPathNorm, dstPathNorm)))
        .collect(Collectors.toList());
    ParallelTaskProcessor.runFailable(srcToDstPaths, parallelism, f -> {
      copyInterruptibly(f.getLeft(), f.getRight(), maxRetries, initialRetrySleepSec, waitTimeFactor);
    });
  }

  /**
   * Copies all files, recursively, from one folder in this bucket to another.
   * Performs the task in parallel, using retries on individual files.
   * Uses default retry settings.
   *
   * @param srcPath source path of the folder to copy, relative to the bucket.
   * Treated as folder, meaning that the caller may or may not append '/' to the path/
   * @param dstPath destination path of the folder to copy into, relative to the bucket.
   * Treated as folder, meaning that the caller may or may not append '/' to the path/
   * @param parallelism The number of threads to use for the task
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of error copying the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void copyFolderRecursiveInterruptibly(String srcPath, String dstPath, int parallelism) throws IOException, InterruptedException {
    copyFolderRecursiveInterruptibly(srcPath, dstPath, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a single object. Nothing happens and no exception is thrown in case the file doesn't exist.
   *
   * @throws IOException In case of an IO error
   * @param objectMeta The metadata object pointing to the remote file to be deleted
   */
  public abstract void delete(T objectMeta) throws IOException;

  /**
   * Deletes a single object. Nothing is done in case the file doesn't exist.
   *
   * @param key The path of the remote file, relative to the bucket
   * @throws IOException In case of an IO error
   */
  public void delete(String key) throws IOException {
    try {
      T meta = getObjectMetadata(key);
      delete(meta);
    } catch (FileNotFoundException e) {
      // Ignore
    }
  }

  /**
   * Deletes a single object with retries. Nothing is done in case the file doesn't exist.
   *
   * @param key The path of the remote file, relative to the bucket
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of an IO error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteInterruptibly(String key, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> delete(key), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Deletes a single object with retries. Nothing is done in case the file doesn't exist.
   *
   * @param objectMeta The metadata object pointing to the remote file to be deleted
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of an IO error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteInterruptibly(T objectMeta, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Retrier.run(() -> delete(objectMeta), initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1);
  }

  /**
   * Deletes a single object with retries. Nothing is done in case the file doesn't exist.
   * Uses default retry settings.
   *
   * @param key The path of the remote file, relative to the bucket
   * @throws IOException In case of an IO error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteInterruptibly(String key) throws IOException, InterruptedException {
    deleteInterruptibly(key, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a single object with retries. Nothing is done in case the file doesn't exist.
   * Uses default retry settings.
   *
   * @param objectMeta The metadata object pointing to the remote file to be deleted
   * @throws IOException In case of an IO error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteInterruptibly(T objectMeta) throws IOException, InterruptedException {
    deleteInterruptibly(objectMeta, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes all regular files under a remote folder.
   *
   * @param path A path to a folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @throws IOException In case of an IO error
   */
  public void deleteFolderRegularFiles(String path) throws IOException {
    path = normalizeFolderPath(path);
    Iterator<T> it = listFiles(path);
    try {
      deleteAllByMetaInterruptibly(it, Runtime.getRuntime().availableProcessors());
    } catch (InterruptedException e) { //TODO(EyalS): change method signature to throw InterruptedException when possible
      throw new InterruptedIOException("Interrupted while deleting remote folder files from '" + path + "'");
    }
  }

  /**
   * Deletes all folder contents, recursively.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param path A path to a folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteFolderRecursiveInterruptibly(String path, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Iterator<T> it = listFilesRecursive(normalizeFolderPath(path));
    deleteAllByMetaInterruptibly(it, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Deletes all folder contents, recursively.
   * Uses default retry settings.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param path A path to a folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param parallelism The number of threads to use for the task
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteFolderRecursiveInterruptibly(String path, int parallelism) throws IOException, InterruptedException {
    deleteFolderRecursiveInterruptibly(path, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a set of remote files.
   * This is the preferred method to use when the number of remote files to delete is large and we want to be memory sensitive.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param fileRefsIt An iterator on file references of all files to delete.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllByMetaInterruptibly(Iterator<T> fileRefsIt, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    List<T> pathsChunk = new ArrayList<>();
    while (fileRefsIt.hasNext()) {
      pathsChunk.clear();
      while (fileRefsIt.hasNext() && pathsChunk.size() < 10_000) {
        pathsChunk.add(fileRefsIt.next());
      }

      deleteAllByMetaInterruptibly(pathsChunk, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
    }
  }

  /**
   * Deletes a set of remote files.
   * This is the preferred method to use when the number of remote files to delete is large and we want to be memory sensitive.
   * Uses default retry settings.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param fileRefsIt An iterator on file references of all files to delete.
   * @param parallelism The number of threads to use for the task
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllByMetaInterruptibly(Iterator<T> fileRefsIt, int parallelism) throws IOException, InterruptedException {
    deleteAllByMetaInterruptibly(fileRefsIt, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a set of remote files. Implementations which support fast batch operations are encouraged to override this method.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param fileRefs The metadata objects pointing to all bucket files to delete
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllByMetaInterruptibly(Collection<T> fileRefs, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    ParallelTaskProcessor.runFailable(fileRefs, parallelism, m -> deleteInterruptibly(m, maxRetries, initialRetrySleepSec, waitTimeFactor));
  }

  /**
   * Deletes a set of remote files.
   * Uses default retry settings.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param fileRefs The metadata objects pointing to all bucket files to delete
   * @param parallelism The number of threads to use for the task
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllByMetaInterruptibly(Collection<T> fileRefs, int parallelism) throws IOException, InterruptedException {
    deleteAllByMetaInterruptibly(fileRefs, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a set of remote files.
   * This is the preferred method to use when the number of remote files to delete is large and we want to be memory sensitive.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param filePathsIt An iterator on paths of all files to delete. Paths are relative to the bucket.
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries on individual files in case of IOException
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllInterruptibly(Iterator<String> filePathsIt, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    List<String> pathsChunk = new ArrayList<>();
    while (filePathsIt.hasNext()) {
      pathsChunk.clear();
      while (filePathsIt.hasNext() && pathsChunk.size() < 10_000) {
        pathsChunk.add(filePathsIt.next());
      }

      Map<String, T> metaObjs = getObjectMetadata(pathsChunk, maxRetries, initialRetrySleepSec, waitTimeFactor);
      List<T> existingObjects = metaObjs.values().stream().filter(Objects::nonNull).collect(Collectors.toList());
      deleteAllByMetaInterruptibly(existingObjects, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
    }
  }

  /**
   * Deletes a set of remote files.
   * This is the preferred method to use when the number of remote files to delete is large and we want to be memory sensitive.
   * Uses default retry settings.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param filePathsIt An iterator on paths of all files to delete. Paths are relative to the bucket.
   * @param parallelism The number of threads to use for the task
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllInterruptibly(Iterator<String> filePathsIt, int parallelism) throws IOException, InterruptedException {
    deleteAllInterruptibly(filePathsIt, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Deletes a set of remote files.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param filePaths The paths of all files to delete, relative to the bucket
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllInterruptibly(Collection<String> filePaths, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    Map<String, T> metaObjs = getObjectMetadata(filePaths, maxRetries, initialRetrySleepSec, waitTimeFactor);
    List<T> existingObjects = metaObjs.values().stream().filter(Objects::nonNull).collect(Collectors.toList());
    deleteAllByMetaInterruptibly(existingObjects, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Deletes a set of remote files.
   * Uses default retry settings.
   *
   * In the default implementation, the 'parallelism' parameter controls the number of threads used for the operation. However,
   * some implementation support fast batch operations and may ignore this parameter.
   *
   * Retry policy depends on the implementation. The default implementation performs retries on individual file level, but implementations
   * may use global level retries.
   *
   * @param filePaths The paths of all files to delete, relative to the bucket
   * @param parallelism The number of threads to use for the task
   * @throws IOException In case of IO error while deleting the files
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void deleteAllInterruptibly(Collection<String> filePaths, int parallelism) throws IOException, InterruptedException {
    deleteAllInterruptibly(filePaths, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Moves a file from one location to another, using retries.
   * By default, implemented as a combination of copy and delete, which means that
   * the operation isn't atomic, and partial results may be seen. However, it is guaranteed that
   * the source file isn't removed before the copy is complete.
   *
   * @param fromKey The path of the source file, relative to the bucket
   * @param toKey The path of the target file, relative to the bucket
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @throws IOException In case of a read/write error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void moveInterruptibly(String fromKey, String toKey, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    copyToAnotherBucketInterruptibly(fromKey, getBucketName(), toKey, maxRetries, initialRetrySleepSec, waitTimeFactor);
    deleteInterruptibly(fromKey, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Moves a file from one location to another, using retries with default settings.
   * By default, implemented as a combination of copy and delete, which means that partial results
   * may be seen. However, it is guaranteed that
   * the source file isn't removed before the copy is complete.
   * Uses default retry settings.
   *
   * @param fromKey The path of the source file, relative to the bucket
   * @param toKey The path of the target file, relative to the bucket
   * @throws IOException in case of a read/write error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void moveInterruptibly(String fromKey, String toKey) throws IOException, InterruptedException {
    moveInterruptibly(fromKey, toKey, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * Moves all files from one folder to another, recursively. Performs the task in parallel, using retries on individual files.
   * By default, implemented as a combination of copy and delete, which means that partial results
   * may be seen. However, it is guaranteed that any of the source files isn't removed before the copy is complete.
   *
   * @param srcPath The path to the source folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param dstPath The path to the destination folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @param parallelism The number of threads to use for the task
   * @throws IOException in case of a read/write error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void moveFolderRecursive(String srcPath, String dstPath, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    copyFolderRecursiveInterruptibly(srcPath, dstPath, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
    deleteFolderRecursiveInterruptibly(srcPath, parallelism, maxRetries, initialRetrySleepSec, waitTimeFactor);
  }

  /**
   * Moves all files from one folder to another, recursively. Performs the task in parallel, using retries on individual files.
   * By default, implemented as a combination of copy and delete, which means that partial results
   * may be seen. However, it is guaranteed that any of the source files isn't removed before the copy is complete.
   * Uses default retry settings.
   *
   * @param srcPath The path to the source folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param dstPath The path to the destination folder, relative to the bucket.
   * The path is treated as a folder anyway, regardless of whether the suffix is '/' or not.
   * @param parallelism The number of threads to use for the task
   * @throws IOException in case of a read/write error
   * @throws InterruptedException In case that the current thread is interrupted
   */
  public void moveFolderRecursive(String srcPath, String dstPath, int parallelism) throws IOException, InterruptedException {
    moveFolderRecursive(srcPath, dstPath, parallelism, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_INITIAL_SLEEP_SEC, DEFAULT_RETRY_WAIT_TIME_FACTOR);
  }

  /**
   * appends to path "/" if not exist already
   */
  public String normalizeFolderPath(String path) {
    if (!isFolderPath(path)) {
      path += "/";
    }
    return path;
  }

  /**
   * @param key file path, relative to the bucket
   * @return true if and only if the file exists in the bucket. In case the key point to a folder (either existing or not),
   * false is returned.
   * @throws IOException upon failure to inspect the bucket
   */
  public abstract boolean exists(String key) throws IOException;

  /**
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param recursive Indicates whether to to return objects recursively beneath the given folder
   * @return The set of all objects (files and folders) under the given path, as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * In case of an IO error during iteration, any runtime exception is allowed, but {@link UncheckedIOException} is recommended.
   * We recommend implementations to return a lazy iterator (with pages in the background) in order to be more memory friendly.
   * @throws IOException upon failure to list the bucket
   */
  public abstract Iterator<T> listObjects(String folderPath, boolean recursive) throws IOException;

  /**
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @return The set of all objects (files and folders) directly under the given path, as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listObjects(String folderPath) throws IOException {
    return listObjects(folderPath, false);
  }

  /**
   * Creates an iterator of all objects directly under provided path
   * that satisfy the provided condition
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param condition A predicate on the listed object, determining whether to include it in the iteration or not.
   * @return The set of all condition-satisfying objects (files/folders) under the given path.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listObjects(String folderPath, Predicate<T> condition) throws IOException {
    Iterator<T> allObjectsIterator = listObjects(folderPath, false);
    return Iterators.filter(allObjectsIterator, condition::test);
  }

  /**
   * Returns an iterator for all the files in a given remote folder, recursively.
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @return The list of all files in the tree of the given path as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFilesRecursive(String folderPath) throws IOException {
    Iterator<T> it = listObjects(folderPath, true);
    return Iterators.filter(it, this::isFile);
  }

  /**
   * Returns an iterator for all the files in a given remote folder matching a regex pattern, recursively.
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param pattern the file pattern to look for, will look in the whole file path (not only file name, but folders too)
   * @return The list of all files in the tree of the given path as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFilesRecursive(String folderPath, Pattern pattern) throws IOException {
    Iterator<T> blobIterator = listFilesRecursive(folderPath);
    Predicate<T> namePatternPredicate = blob -> pattern.matcher(getPath(blob)).matches();
    return Iterators.filter(blobIterator, namePatternPredicate::test);
  }

  /**
   * Returns an iterator for all the files in a given remote folder matching the pattern, recursively.
   *
   * @param folderPath A path under the bucket.
   * @param fileRegex the file pattern to look for, will look in the whole file path (not only file name, but folders too)
   * @return The list of all files in the tree of the given path as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFilesRecursive(String folderPath, String fileRegex) throws IOException {
    return listFilesRecursive(folderPath, Pattern.compile(fileRegex));
  }

  /**
   * Lists all files directly under the given path (excluding folders)
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @return The list of all files under the given path, as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFiles(String folderPath) throws IOException {
    return listObjects(folderPath, this::isFile);
  }

  /**
   * Lists all files that match the regex pattern and are located directly under the given path
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param filePattern regex pattern to match the file name (without path) and filter results by
   * @return The list of all objects in the given path as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFiles(String folderPath, Pattern filePattern) throws IOException {
    Predicate<T> matchingFilesPredicate =
        ((Predicate<T>)this::isFile).and(
            blob -> filePattern.matcher(PathUtils.getLastPathPart(getPath(blob))).matches());
    return listObjects(folderPath, matchingFilesPredicate);
  }

  /**
   * Lists all sub-folders in the given path
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @return The list of all folder objects under the given path.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFolders(String folderPath) throws IOException {
    return listObjects(folderPath, ((Predicate<T>) this::isFile).negate());
  }

  /**
   * Lists all files directly under the given path that match the regular expression.
   *
   * @param folderPath A folder path, relative to the bucket.
   * Treated as a folder path - the caller may or may not add '/' to it.
   * @param fileRegex regular expression to match the file name (without path) and filter results by
   * @return The list of all objects in the given path, as an iterator.
   * In case that the path doesn't exist, returns an empty iterator. No particular order is guaranteed.
   * @throws IOException upon failure to list the bucket
   */
  public Iterator<T> listFiles(String folderPath, String fileRegex) throws IOException {
    return listFiles(folderPath, Pattern.compile(fileRegex));
  }

  /**
   * Returns the maximal (lexicographic ordering of the full path string) file in the path,
   * filtering by given pattern. The search is recursive.
   *
   * @param path the base path to look for, relative to the bucket
   * @param pattern the full path pattern (relative to bucket) to match against
   * @return the metadata object corresponding the the last file, or null if not found
   * @throws IOException In case of IO error when listing the files in the given path
   */
  public T getLastFile(String path, Pattern pattern) throws IOException {
    Optional<T> max = Streams.stream(listFilesRecursive(path, pattern)).max(Comparator.comparing(this::getPath));
    return max.orElse(null);
  }

  /**
   * Supplies the metadata of a file, if it exists
   *
   * @param filePath the path for the file, relative to the current bucket
   * @return the metadata of the required object
   * @throws IOException upon failure to retrieve the metadata for the specified file
   * @throws FileNotFoundException if the provided path refers to a non existing remote file, or if it refers to a folder
   */
  public abstract T getObjectMetadata(String filePath) throws IOException;

  /**
   * Supplies metadata objects for a given set of remote file paths.
   *
   * This basic implementation delegates to getObjectMetadata(String), and runs requests in parallel (using #cores threads).
   * Implementations are encouraged to override this method with a more efficient batch operation if available.
   *
   * @param filePaths The remote paths to fetch the metadata objects for
   * @return The metadata objects map, where keys are input paths and values are corresponding metadata objects.
   * Nulls will be returned for a path if and only if that path doesn't exist.
   * @throws IOException upon storage failure while trying to retrieve the metadata objects
   * @throws InterruptedException in case that the thread is interrupted
   */
  public Map<String, T> getObjectMetadata(Collection<String> filePaths) throws IOException, InterruptedException {
    Map<String, T> res = Collections.synchronizedMap(new HashMap<>());
    ParallelTaskProcessor.runFailable(filePaths, Runtime.getRuntime().availableProcessors(), path -> {
      try {
        T meta = getObjectMetadata(path);
        res.put(path, meta);
      } catch (FileNotFoundException e) {
        res.put(path, null);
      }
    });
    return res;
  }

  /**
   * Supplies metadata objects for a given set of remote file paths.
   *
   * This basic implementation delegates to getObjectMetadata(String), and runs requests in parallel (using #cores threads).
   * Implementations are encouraged to override this method with a more efficient batch operation if available.
   *
   * @param filePaths The remote paths to fetch the metadata objects for
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep before the first retry
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The metadata objects map, where keys are input paths and values are corresponding metadata objects.
   * Nulls will be returned for a path if and only if that path doesn't exist.
   * @throws IOException upon storage failure while trying to retrieve the metadata objects
   * @throws InterruptedException in case that the thread is interrupted
   */
  public Map<String, T> getObjectMetadata(Collection<String> filePaths, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    MutableObject<Map<String, T>> metaObjs = new MutableObject<>();
    Retrier.run(() -> metaObjs.setValue(getObjectMetadata(filePaths)),initialRetrySleepSec * 1000, waitTimeFactor,maxRetries + 1);
    return metaObjs.getValue();
  }

  /**
   * @param objMetadata A metadata of an object under the bucket
   * @return The path of the object represented by this metadata (relative to the bucket). The path always uses '/' as folder separator.
   * In case that the metadata object represents a folder (and only in this case), the path should be terminated with '/'.
   * In case the metadata indicates the object doesn't belong to the current bucket, {@link IllegalArgumentException} is thrown.
   */
  public abstract String getPath(T objMetadata);

  /**
   * @param objMetadata A metadata of an object under the bucket or another
   * @return The size in bytes of the object as specified by the metadata.
   * In case the metadata object refers to a folder, 0 should be returned.
   */
  public abstract long getLength(T objMetadata);

  /**
   * @param objMetadata A metadata of an object under the bucket or another
   * @return The time the file was last updated, as milliseconds since epoch
   * In case the metadata object refers to a folder, null should be returned.
   */
  public abstract Long getLastUpdated(T objMetadata);

  /**
   * @param objMetadata A metadata of an obhect under this bucket
   * @return true if and only if the metadata object refers to a file.
   * We consider any objects with path ending with "/" as folders, and others as files.
   * In case the object doesn't belong to the current bucket, {@link IllegalArgumentException} is thrown.
   */
  public boolean isFile(T objMetadata) {
    return !isFolderPath(getPath(objMetadata));
  }

  /**
   * <p>Generates a signed url for an upload on a specific file. This generated url will expire within
   * the required number of seconds.<br> This generated url only allows the client to use <i>PUT</i>
   * operation on it.</p>
   * The file will have private read access.
   *
   * @param key the target file path, relative to the bucket
   * @param contentType the key's content type. this content type must be supplied by the client
   * when uploading to the url.
   * @param expirationSeconds number of seconds during which the sign url is valid
   * @return The signed url for the required key
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public URL generateSignedUrl(String key, String contentType, int expirationSeconds) {
    return generateSignedUrl(key, contentType, expirationSeconds, false);
  }

  /**
   * <p>Generates a signed url for an upload on a specific file. This generated url will expire within
   * the required number of seconds.<br> This generated url only allows the client to use <i>PUT</i>
   * operation on it.</p>
   *
   * @param key the target key, relative to the bucket
   * @param contentType the key's content type. this content type must be supplied by the client
   * when uploading to the url.
   * @param expirationSeconds number of seconds in which the sign url is valid
   * @param isPublic true to set public file access, false for private
   * @return a signed url for the required key
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public abstract URL generateSignedUrl(String key, String contentType, int expirationSeconds, boolean isPublic);

  /**
   * <p>Generates a signed url for reading a specific file. This generated url will expire within
   * the required number of seconds.<br>
   * This generated url only allows the client read (GET) from this url.
   *
   * @param key the remote file path, relative to the bucket
   * @param expirationSeconds number of seconds during which the sign url is valid
   * @return a signed url for the required file
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public abstract URL generateReadOnlyUrl(String key, int expirationSeconds);

  /**
   * <p>Generates a resumable signed url for a for uploading to a specific file for private access. This generated url will expire within
   * the required number of seconds.<br> This generated url only allows the client to use <i>PUT</i>
   * operation on it.</p>
   * Should be used if cannot anticipate the size of the uploaded file or given to a non-third party,
   * otherwise use the size limited one.
   *
   * @param key the target file the sign url points to
   * @param contentType the key's content type. this content type must be supplied by the client
   * when uploading to the url.
   * @param expirationSeconds number of seconds in which the sign url is valid
   * @return a signed url for the required key
   * @throws IOException if could not sign url
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public URL generateResumableSignedUrlForUpload(String key, String contentType, int expirationSeconds) throws IOException {
    return generateResumableSignedUrlForUpload(key, contentType, expirationSeconds, UNLIMITED_CONTENT_LENGTH, false);
  }

  /**
   * <p>Generates a resumable signed url for a for uploading to a specific file. This generated url will expire within
   * the required number of seconds.<br> This generated url only allows the client to use <i>PUT</i>
   * operation on it.</p>
   * The upload file is limited only to the declared size in byte if maxContentLengthInBytes is not null.
   *
   * @param key the target file the sign url points to
   * @param contentType the key's content type. this content type must be supplied by the client
   * when uploading to the url.
   * @param expirationSeconds number of seconds in which the sign url is valid
   * @param maxContentLengthInBytes if not null then limits the uploaded content bytes.
   * @param isPublic true to set public file access, false for private.
   * @return a signed url for the required key
   * @throws IOException if could not sign url
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public abstract URL generateResumableSignedUrlForUpload(String key, String contentType, int expirationSeconds, Long maxContentLengthInBytes, boolean isPublic) throws IOException;

  /**
   * Composes (concats) remote files.
   * This operation is done remotely, and typically performed in an efficient manner by
   * creating a virtual file rather than copying data. 
   * Important notes:
   * 1) Be careful when composing files, since not all file formats (specially compressed ones) are valid after
   * concatenation.
   * 2) The final file is created atomically, but remote intermediate files may be created in the
   * target folder during the operation. The intermediate files are removed (best effort) once the operation terminates,
   * either successfully or not.
   *
   * @param paths The non-empty list of paths (relative to the bucket) of files to compose, in the required order
   * @param composedFilePath The target path (relative to the bucket) of the composed file
   * @param removeComprisingFiles Whether to remove the files that were concatenated. Deletion is done only 
   * after successful composing of all files.
   * @return The path to the composed object, relative to the bucket. May be one of the input files (for achieving kind of append functionality for example). 
   * Overridden if already exists.
   * @throws IOException In case of IO error, or if the supplied target file has a form of a folder rather than a file
   * @throws UnsupportedOperationException In case that the specific implementation doesn't support this operation
   */
  public abstract T compose(List<String> paths, String composedFilePath, boolean removeComprisingFiles) throws IOException;

  /**
   * @param path A remote path, expected to have a file form
   * @throws IOException In case that the path has the form of a folder path
   */
  protected void validateNotFolderPath(String path) throws IOException {
    if (isFolderPath(path)) {
      throw new IOException("Illegal path: '" + path + "'. Expected a file path.");
    }
  }

  /**
   * @param path A remote path
   * @return true if and only if the path ends with "/"
   */
  public boolean isFolderPath(String path) {
    return path.endsWith("/");
  }

  /**
   * @param path A remote object path 
   * @return true if and only if the object refers to a file.
   * We consider any path ending with "/" as denoting a folder, and others as files.
   */
  public boolean isFilePath(String path) {
    return !isFolderPath(path);
  }

  private static String generateUniqueKey(String path, String fileExtension) {
    return PathUtils.buildPath(path, Long.toHexString(ThreadLocalRandom.current().nextLong()) + fileExtension);
  }

  private void validateAllPathsExist(Map<String,T> pathsToMeta) throws FileNotFoundException {
    Optional<String> nonExistingPath = pathsToMeta.entrySet().stream().filter(x -> x.getValue() == null).map(Entry::getKey).findFirst();
    if (nonExistingPath.isPresent()) {
      throw new FileNotFoundException("File doesn't exist: " + nonExistingPath.get());
    }
  }
}
