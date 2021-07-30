package org.pipecraft.infra.storage.google_cs;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.auth.ServiceAccountSigner.SigningException;
import com.google.cloud.BatchResult.Callback;
import com.google.cloud.ReadChannel;
import com.google.cloud.RetryHelper.RetryHelperException;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Role;
import com.google.cloud.storage.Acl.User;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobInfo.Builder;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import org.pipecraft.infra.concurrent.FailableInterruptibleConsumer;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.Retrier;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.storage.IllegalJsonException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage bucket implementation based on Google Storage.
 * 
 * This implementation supports all optional {@link Bucket} operations, and maintains
 * List-after-write consistency.
 *
 * @author Oren Peer, Eyal Schneider
 */
public class GoogleStorageBucket extends Bucket<Blob> {

  private final static Logger LOGGER = LoggerFactory.getLogger(GoogleStorageBucket.class);

  public static final String X_GOOG_ACL_HEADER = "x-goog-acl";
  public static final String ACL_PUBLIC_READ = "public-read";
  public static final String X_GOOG_RESUMABLE_HEADER = "x-goog-resumable";
  public static final String X_GOOG_CONTENT_LENGTH_RANGE = "x-goog-content-length-range";

  private static final BlobWriteOption[] BLOB_WRITE_OPTION_DOES_NOT_EXIST = new BlobWriteOption[] {BlobWriteOption.doesNotExist()};
  private static final BlobWriteOption[] BLOB_WRITE_OPTIONS_EMPTY = new BlobWriteOption[0];
  private static final long MAX_EXPIRATION_SECONDS = TimeUnit.DAYS.toSeconds(7);

  private static final int COMPOSE_FILE_COUNT_LIMIT = 32;
  private static final int COMPOSE_MAX_PARALLELISM = 16;
  private static final int MEGABYTE = 1024 * 1024;
  private static final int SLICED_DOWNLOAD_DEFAULT_CHUNK_SIZE = 2 * MEGABYTE;
  private static final int SLICED_DOWNLOAD_DEFAULT_PARALLELISM = 20;
  private static final int SLICED_DOWNLOAD_SLICE_SIZE = 20 * MEGABYTE; // The slice size we try to achieve when doing sliced download. The actual slice size may be smaller/larger in some edge cases.
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final List<Acl> PUBLIC_READ_ACL = Collections.singletonList(Acl.of(User.ofAllUsers(), Role.READER));

  private final Storage storage;

  /**
   * Constructor
   * 
   * @param storage The 
   * @param bucketName The bucket name
   */
  GoogleStorageBucket(Storage storage, String bucketName) {
    super(bucketName);
    this.storage = storage;
  }

  // This implementation supports the optional allowOverride feature, and doesn't require the length parameter
  @Override
  public void put(String key, InputStream input, long length, String contentType, boolean isPublic, boolean allowOverride) throws IOException {
    try (InputStream is = input) {
      validateNotFolderPath(key);
      
      BlobInfo.Builder blobInfo = BlobInfo.newBuilder(getBucketName(), key);
      if (contentType != null) {
        blobInfo.setContentType(contentType);
      }
      if (isPublic) {
        blobInfo.setAcl(PUBLIC_READ_ACL);
      }
      BlobInfo blob = blobInfo.build();

      BlobWriteOption[] blobTargetOptions = allowOverride ? BLOB_WRITE_OPTIONS_EMPTY : BLOB_WRITE_OPTION_DOES_NOT_EXIST;
      try (WriteChannel writer = storage.writer(blob, blobTargetOptions)) {
        byte[] buffer = new byte[BUFFER_SIZE];
        int read;
        while ((read = is.read(buffer)) >= 0) {
          writer.write(ByteBuffer.wrap(buffer, 0, read));
        }
      }
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed uploading to " + getBucketName() + "/" + key);
    }
  }

  @Override
  public OutputStream getOutputStream(String key, int chunkSize) throws IOException {
    validateNotFolderPath(key);
    
    WriteChannel writer;
    try {
      BlobInfo blobInfo = BlobInfo.newBuilder(getBucketName(), key).build();
      writer = storage.writer(blobInfo);
      if (chunkSize > 0) {
        writer.setChunkSize(chunkSize);
      }
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed getting output stream for '" + getBucketName() + "/" + key + "'");
    }
    return new GSOutputStream(Channels.newOutputStream(writer));
  }

  @Override
  public void get(Blob meta, File output) throws IOException {
    Path path = output.toPath();
    try {
      meta.downloadTo(path);
    } catch (StorageException | RetryHelperException e) { // The RetryHelperException must be caught here. Bad (and undocumented!) API choice for Blob.downloadTo(..).)
      throw mapToIOException(extractStorageException(e), "Failed getting " + getBucketName() + "/" + meta.getName());
    } 
  }

  /**
   * Downloads a file from Google Storage, using sliced download.
   * Retries are performed on individual slices.
   *
   * @param ex The executor to use for downloading the file
   * @param key Google Storage key name (not including bucket)
   * @param output The target file to write to (overriding write).
   * The file may not exist, but the folder must exist.
   * @param chunkSize The size (in bytes) of each chunk read from GS at once, or 0 for using the default one. It is recommended that the chunkSize size be between 2MB and 6MB. the memory usage will be
   * at least MAX_THREAD_FOR_DOWNLOAD * chunkSize
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file, or in case that the thread has been interrupted
   * @throws InterruptedException in case of an interruption while waiting for all slices to be downloaded
   */
  public void get(ExecutorService ex, String key, File output, int chunkSize) throws IOException, InterruptedException {
    List<SliceTransferJobDetails> sliceJobs = new ArrayList<>();
    try {
      Blob blob = getObjectMetadata(key);
      createSlicedJobs(blob, output, chunkSize, sliceJobs);
      ParallelTaskProcessor.runFailable(ex, sliceJobs, new SliceReaderTask(DEFAULT_RETRIER));
    } catch (FileNotFoundException e) {
      throw e; // We want to make sure that FileNotFoundException isn't wrapped
    } catch (StorageException | IOException e) {
      throw mapToIOException(extractStorageException(e), "Failed getting " + getBucketName() + "/" + key);
    } finally {
      FileUtils.close(sliceJobs);
    }
  }

  /**
   * Downloads a file from Google Storage, using sliced download
   *
   * @param ex The executor to use for downloading the file
   * @param key Google Storage key name (not including bucket)
   * @param output The target file to write to
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file
   * @throws InterruptedException in case of an interruption while waiting for all slices to be downloaded
   */
  public void get(ExecutorService ex, String key, File output) throws IOException, InterruptedException {
    get(ex, key, output, 0);
  }

  /**
   * Downloads a file from Google Storage, using sliced download.
   * Uses a temporary pooled executor.
   *
   * @param key Google Storage key name (not including bucket)
   * @param output The target file to write to
   * @param chunkSize The size (in bytes) of each chunk read from GS at once, or 0 for using the default one. It is recommended that the chunkSize size be between 2MB and 6MB. the memory usage will be
   * at least MAX_THREAD_FOR_DOWNLOAD * chunkSize
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file
   * @throws InterruptedException in case of an interruption while waiting for all slices to be downloaded
   */
  @Override
  public void getSliced(String key, File output, int chunkSize) throws IOException, InterruptedException {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("Sliced-download-%d").build();
    ExecutorService ex = new ThreadPoolExecutor(SLICED_DOWNLOAD_DEFAULT_PARALLELISM, SLICED_DOWNLOAD_DEFAULT_PARALLELISM, 0L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), tf);
    try {
      get(ex, key, output, chunkSize);
    } finally {
      ex.shutdownNow();
    }
  }

  /**
   * Downloads a file from Google Storage, using sliced download.
   * Uses a temporary pooled executor, and default chunk size.
   *
   * @param key Google Storage key name (not including bucket)
   * @param output The target file to write to
   * @throws FileNotFoundException In case that the key wasn't found in the bucket
   * @throws IOException In case of IO error while reading the file, or if the thread is interrupted while waiting
   * for all slices to download 
   * @deprecated Use the other get(ExecutorService ..) or getSliced(..) methods, as they handles better interrupted exceptions 
   */
  @Override
  @Deprecated
  public void getSliced(String key, File output) throws IOException {
    try {
      getSliced(key, output, 0);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while doing a sliced download of '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public <C> C getFromJson(String key, Class<C> clazz) throws IOException, IllegalJsonException {
    try {
      return super.getFromJson(key, clazz);
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed getting JSON from '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public SizedInputStream getAsStream(Blob meta, int chunkSize) throws IOException {
    ReadChannel reader;
    try {
      reader = meta.reader();
      if (chunkSize > 0) {
        reader.setChunkSize(chunkSize);
      }
      return new SizedInputStream(new GSInputStream(Channels.newInputStream(reader)), meta.getSize());
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed getting input stream for '" + getBucketName() + "/" + meta.getName() + "'");
    }
  }

  /**
   * Retrieves a set of files from different paths, in an efficient manner.
   * This method is intended to speed up multi file download, by applying concurrent sliced download on multiple files.
   *  
   * The implementation bounds the total number of file handles (connections+local files) it opens at once.
   * The bound is twice the value passed in the parallelism parameter.
   *  
   * Expects a fileNameResolver, which allows assigning different names to target files. This may be useful
   * for example when protecting from remote files with the same name to override each other.
   *
   * @param metaObjects The collection of objects pointing to the remote files to download
   * @param targetFolder The local target folder to write files to. Created if needed.
   * @param fileNameResolver A mapper between remote path (relative to bucket) to the local file name to assign to it (just name, without path).
   * @param parallelism The number of threads to use for the task
   * @param maxRetries Maximum number of retries in case of IOException (except for {@link FileNotFoundException} which won't trigger retries).
   * @param initialRetrySleepSec The initial number of seconds to sleep between retries (increases exponentially)
   * @param waitTimeFactor A factor by which the sleep times between retries increases (millisecond precision)
   * @return The set of downloaded file objects
   * @throws FileNotFoundException In case that the one of the paths wasn't found in the bucket
   * @throws IOException In case of IO error while downloading the files
   * @throws InterruptedException In case that the thread is interrupted
   */
  @Override
  public Set<File> getAllRegularFilesByMetaInterruptibly(Collection<Blob> metaObjects, File targetFolder, Function<String, String> fileNameResolver, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    targetFolder.mkdirs();
    if (!targetFolder.isDirectory()) {
      throw new IOException("Not a folder: " + targetFolder);
    }
    
    Set<File> res = new HashSet<>();
    List<SliceTransferJobDetails> sliceJobs = new ArrayList<>();
    for (Blob blob : metaObjects) {
      File localFile = new File(targetFolder, fileNameResolver.apply(blob.getName()));
      res.add(localFile);
      createSlicedJobs(blob, localFile, 0, sliceJobs); // 0 for using default chunk size
    }

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("Sliced-download-multi-%d").build();
    // Important: the TPE queue must remain FIFO for the open files bound guarantee to hold.
    // Only when slice jobs are ordered by files, we can guarantee that no more than #threads files are opened at once.
    ExecutorService ex = new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), tf);

    try {
      ParallelTaskProcessor.runFailable(ex, sliceJobs, new SliceReaderTask(new Retrier(initialRetrySleepSec * 1000, waitTimeFactor, maxRetries + 1)));
    } catch (FileNotFoundException e) {
      throw e; // We want to make sure that FileNotFoundException isn't wrapped
    } catch (StorageException | IOException e) {
      throw mapToIOException(extractStorageException(e), "Failed downloading all " + metaObjects.size() + " files from bucket " + getBucketName());
    } finally {
      FileUtils.close(sliceJobs);
      ex.shutdownNow();
    }

    return res;
  }

  @Override
  public void copyToAnotherBucket(String fromKey, String toBucket, String toKey) throws IOException {
    BlobId targetBlob = BlobId.of(toBucket, toKey);
    Path fromPath = Paths.get(getBucketName(), fromKey);
    Path toPath = Paths.get(toBucket, toKey);
    LOGGER.debug("Copying from " + fromPath.toString() + " to " + toPath.toString());
    CopyRequest copyRequest = CopyRequest.of(getBucketName(), fromKey, targetBlob);

    try {
      storage.copy(copyRequest).getResult();
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed copying from '" + getBucketName() + "/" + fromKey + "' to '" + toBucket + "/" + toKey + "'");
    }
  }

  @Override
  public void delete(Blob obj) throws IOException {
    try {
      storage.delete(obj.getBlobId());
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed deleting '" + getBucketName() + "/" + obj.getName() + "'");
    }
  }

  @Override
  public void deleteAllByMetaInterruptibly(Collection<Blob> fileRefs, int parallelism, int maxRetries, int initialRetrySleepSec, double waitTimeFactor) throws IOException, InterruptedException {
    try {
      AtomicReference<StorageException> exception = new AtomicReference<>();
      CountDownLatch terminationLatch = new CountDownLatch(fileRefs.size());
      StorageBatch batch = storage.batch();
      for (Blob meta : fileRefs) {
        StorageBatchResult<Boolean> r = batch.delete(meta.getBlobId());
        r.notify(new Callback<>() {
          @Override
          public void success(Boolean resultBlob) {
            terminationLatch.countDown();
          }

          @Override
          public void error(StorageException storageException) {
            exception.compareAndSet(null, storageException);
            // Release latch as soon as possible in order to exit with an error
            long nonCompleted = Math.max(1, terminationLatch.getCount()); // We don't know how robust is getCount(), and whether it may return 0.
            for (int i = 0; i < nonCompleted; i++) {
              terminationLatch.countDown();
            }
          }
        });
      }
      batch.submit();
      terminationLatch.await();
      StorageException thrownExc = exception.get();
      if (thrownExc != null) {
        throw thrownExc;
      }
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed submitting delete batch request");
    }
  }

  @Override
  public boolean exists(String key) throws IOException {
    BlobId blobId = BlobId.of(getBucketName(), key);
    try {
      return storage.get(blobId) != null && isFilePath(key);
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed checking existence of '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public Iterator<Blob> listObjects(String folderPath, boolean recursive) throws IOException {
    Iterator<Blob> it;
    try {
      String normalizedPath = normalizeFolderPath(folderPath);
      com.google.cloud.storage.Bucket bucket = storage.get(this.getBucketName());
      BlobListOption prefixOption = BlobListOption.prefix(normalizedPath);
      BlobListOption[] options = recursive ? 
          new BlobListOption[] {prefixOption} : new BlobListOption[] {prefixOption, BlobListOption.currentDirectory()};
      it = bucket.list(options).iterateAll().iterator();
      it = Iterators.filter(it, b -> !b.getName().equals(normalizedPath)); // Filter out the folder itself
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed listing '" + getBucketName() + "/" + folderPath + "'");
    }
    return it;
  }

  @Override
  public URL generateSignedUrl(String key, String contentType, int expirationSeconds, boolean isPublicRead) {
    Builder builder = BlobInfo.newBuilder(getBucketName(), key).setContentType(contentType);
    if (isPublicRead) {
      builder.setAcl(PUBLIC_READ_ACL);
    }
    BlobInfo blobInfo = builder.build();
    Blob blob = storage.create(blobInfo);

    return blob.signUrl(expirationSeconds, SECONDS, SignUrlOption.withContentType(),
        SignUrlOption.httpMethod(HttpMethod.PUT));
  }

  @Override
  public URL generateReadOnlyUrl(String key, int expirationSeconds) {
    BlobId blobId = BlobId.of(getBucketName(), key);
    Blob blob = storage.get(blobId);
    return blob.signUrl(expirationSeconds, SECONDS, SignUrlOption.httpMethod(HttpMethod.GET));
  }

  @Override
  public URL generateResumableSignedUrlForUpload(String key, String contentType, int expirationSeconds, Long maxContentLengthInBytes, boolean isPublic) throws IOException {
    if (expirationSeconds > MAX_EXPIRATION_SECONDS) {
      throw new IllegalArgumentException("Expiration Time can't be longer than " + MAX_EXPIRATION_SECONDS + " seconds");
    }

    // Define the target resource to upload
    BlobId blobId = BlobId.of(getBucketName(), key);
    BlobInfo blobinfo = BlobInfo.newBuilder(blobId).build();

    //sign the url
    Map<String, String> extensionHeaders = new HashMap<>();
    extensionHeaders.put("Content-Type", contentType);
    extensionHeaders.put(X_GOOG_RESUMABLE_HEADER, "start");

    if (maxContentLengthInBytes != null) {
      extensionHeaders.put(X_GOOG_CONTENT_LENGTH_RANGE, "0," + maxContentLengthInBytes);
    }

    if (isPublic) {
      extensionHeaders.put(X_GOOG_ACL_HEADER, ACL_PUBLIC_READ);
    }

    try {
      return storage.signUrl(
          blobinfo,
          expirationSeconds,
          SECONDS,
          Storage.SignUrlOption.httpMethod(HttpMethod.POST),
          Storage.SignUrlOption.withExtHeaders(extensionHeaders),
          Storage.SignUrlOption.withV4Signature());
    } catch (SigningException e) {
      throw new IOException("Could not sign url", e);
    }
  }

  @Override
  public Blob getObjectMetadata(String key) throws IOException {
    BlobId blobId = BlobId.of(getBucketName(), key);
    try {
      Blob res = storage.get(blobId);
      if (res == null || res.isDirectory()) {
        throw new FileNotFoundException("File not found: '" + getBucketName() + "/" + key + "'");
      }
      return res;
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed getting metadata of file '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public Map<String, Blob> getObjectMetadata(Collection<String> filePaths) throws IOException, InterruptedException {
    Map<String, Blob> res = Collections.synchronizedMap(new HashMap<>());
    try {
      AtomicReference<StorageException> exception = new AtomicReference<>();
      CountDownLatch terminationLatch = new CountDownLatch(filePaths.size());
      StorageBatch batch = storage.batch();
      for (String path : filePaths) {
        StorageBatchResult<Blob> r = batch.get(getBucketName(), path);
        r.notify(new Callback<>() {
          @Override
          public void success(Blob resultBlob) {
            res.put(path, resultBlob);
            terminationLatch.countDown();
          }

          @Override
          public void error(StorageException storageException) {
            exception.compareAndSet(null, storageException);
            // Release latch as soon as possible in order to exit with an error
            long nonCompleted = Math.max(1, terminationLatch.getCount()); // We don't know how robust is getCount(), and whether it may return 0.
            for (int i = 0; i < nonCompleted; i++) {
              terminationLatch.countDown();
            }
          }
        });
      }
      batch.submit();
      terminationLatch.await();
      StorageException thrownExc = exception.get();
      if (thrownExc != null) {
        throw thrownExc;
      }
      return res;
    } catch (StorageException se) {
      throw mapToIOException(se, "Failed submitting batch request for metadata");
    }
  }

  @Override
  public String getPath(Blob keyMetadata) {
    if (!keyMetadata.getBucket().equals(getBucketName())) {
      throw new IllegalArgumentException("The given Blob belongs to a different bucket ('" + keyMetadata.getBucket() + "')");
    }
    return keyMetadata.getName();
  }

  @Override
  public long getLength(Blob keyMetadata) {
    return keyMetadata.getSize();
  }

  @Override
  public Long getLastUpdated(Blob keyMetadata) {
    if (keyMetadata.isDirectory()) {
      return null;
    }
    return keyMetadata.getUpdateTime();
  }

  private Blob composeChunk(List<String> gsPaths, String composedFilePath) throws IOException {
    ComposeRequest composeRequest = ComposeRequest.newBuilder()
        .addSource(gsPaths)
        .setTarget(BlobInfo.newBuilder(getBucketName(), composedFilePath).setContentType("text/plain").build())
        .build();
    try {
      return storage.compose(composeRequest);
    } catch (StorageException e) {
      throw mapToIOException(e, "Failed composing files");
    }
  }

  @Override
  public Blob compose(List<String> gsPaths, String composedFilePath, boolean removeComprisingFiles) throws IOException {
    ExecutorService executor = null;
    try {
      validateNotFolderPath(composedFilePath);

      List<String> composeQueue = gsPaths;
      List<String> tmpFiles = new Vector<>(gsPaths.size() * 2);
      executor = Executors.newFixedThreadPool(Math.min(gsPaths.size(), COMPOSE_MAX_PARALLELISM));
      while (composeQueue.size() > COMPOSE_FILE_COUNT_LIMIT) {
        List<String> inputPaths = composeQueue;
        List<Integer> indices = IntStream.iterate(0, i -> i < inputPaths.size(), i -> i + COMPOSE_FILE_COUNT_LIMIT).boxed().collect(Collectors.toList());
        List<String> outputPaths = new Vector<>(Collections.nCopies(indices.size(), null));
        ParallelTaskProcessor.runFailable(executor, indices, i -> {
          if (i == inputPaths.size() - 1) {
            outputPaths.set(outputPaths.size() - 1, inputPaths.get(i));
            return;
          }
          String tmpFile = composedFilePath + "." + Integer.toHexString(ThreadLocalRandom.current().nextInt()) + ".tmp";
          composeChunk(inputPaths.subList(i, Math.min(i + COMPOSE_FILE_COUNT_LIMIT, inputPaths.size())), tmpFile);
          outputPaths.set(i / COMPOSE_FILE_COUNT_LIMIT, tmpFile);
          tmpFiles.add(tmpFile);
        });
        composeQueue = outputPaths;
      }
      Blob res = composeChunk(composeQueue, composedFilePath);
  
      // Cleanup
      if (removeComprisingFiles) {
        gsPaths.stream().filter(path -> !path.equals(composedFilePath)).forEach(tmpFiles::add); // Avoid deleting the target file if it also appears as input
      }
      deleteAllInterruptibly(tmpFiles, Runtime.getRuntime().availableProcessors());
      
      return res;

    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while operating on remote files");
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }
  
  // Scans the exception chain, and looks for the first StorageException under it.
  // Returns null if not found
  private static StorageException extractStorageException(Throwable e) {
    while (!(e instanceof StorageException)) {
      e = e.getCause();
      if (e == null) {
        return null;
      }
    }
    return (StorageException) e;
  }
  
  // Converts a StorageException to an IOException, trying to use a specific one if possible.
  // Accepts nulls.
  private static IOException mapToIOException(StorageException e, String msg) {
    if (e == null) {
      return new IOException(msg);
    }
    switch (e.getCode()) {
      case HttpStatus.SC_NOT_FOUND: return new FileNotFoundException(msg + ". Remote file not found.");
      case HttpStatus.SC_PRECONDITION_FAILED : return new FileAlreadyExistsException(msg + ". File already exists.");
    }
    return new IOException(msg, e);
  }

  private void createSlicedJobs(Blob blob, File output, int chunkSize, List<SliceTransferJobDetails> sliceJobs) {
    if (chunkSize < 0) {
      throw new IllegalArgumentException("Invalid chunkSize: " + chunkSize);
    } else if (chunkSize == 0) {
      chunkSize = SLICED_DOWNLOAD_DEFAULT_CHUNK_SIZE;
    }

    long fileSize = blob.getSize();
    if (fileSize == 0) {
      sliceJobs.add(new SliceTransferJobDetails(blob, chunkSize, new SlicedTransferFileHandler(output, 1), 0, 0));
    } else {
      long sliceSize = Math.min(fileSize, Math.max(chunkSize, SLICED_DOWNLOAD_SLICE_SIZE));
      int numberOfSlices = (int) (fileSize / sliceSize);
      long lastSliceSize = fileSize % sliceSize;
      if (lastSliceSize != 0) {
        numberOfSlices++;
      }

      SlicedTransferFileHandler fileHandler = new SlicedTransferFileHandler(output, numberOfSlices);
      for (int i = 0; i < numberOfSlices; i++) {
        long length = (i == numberOfSlices - 1 && lastSliceSize != 0) ? lastSliceSize : sliceSize;
        sliceJobs.add(new SliceTransferJobDetails(blob, chunkSize, fileHandler, i * sliceSize, length));
      }
    }
  }

  private static class SliceReaderTask implements FailableInterruptibleConsumer<SliceTransferJobDetails, IOException> {
    private final Retrier retrier;

    public SliceReaderTask(Retrier retrier) {
      this.retrier = retrier;
    }
    
    @Override
    public void accept(SliceTransferJobDetails slice) throws IOException {
      MutableObject<ReadChannel> readerRef = new MutableObject<>();
      try {
        retrier.run(() -> {
          try {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            ReadChannel reader = slice.getReadChannel();
            readerRef.setValue(reader);
            FileChannel writer = slice.getTargetFileHandler().getWriter(); // Lazy and single opening of the file channel
            long sliceSize = slice.getLength();
            long currentPosition = slice.getPosition();
            reader.seek(currentPosition);
            int totalBytesRead = 0;
            setBufferLimit(buffer, totalBytesRead, sliceSize);
            
            while (totalBytesRead < sliceSize) {
              int readBytes = reader.read(buffer);
              if (readBytes == -1) {
                throw new IOException("Premature end of file. Read " + totalBytesRead + ", but expected " + (sliceSize - totalBytesRead) + " more bytes.");
              }
              buffer.flip();
              writer.write(buffer, currentPosition);
              currentPosition += readBytes;
              buffer.clear();
              totalBytesRead += readBytes;
              setBufferLimit(buffer, totalBytesRead, sliceSize);
            }
            slice.getTargetFileHandler().doneSliceProcessing(); // Keeps track of each file's slices in order to close the file when done
          } catch (StorageException e) {
            throw mapToIOException(e, "Slice download failed");
          }
        });
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      } finally {
        FileUtils.close(readerRef.getValue());
      }
    }

    private static void setBufferLimit(ByteBuffer buffer, long totalBytesRead, long sliceSize) {
      if (totalBytesRead + BUFFER_SIZE > sliceSize) {
        int limit = (int) (sliceSize - totalBytesRead); //limit <= BUFFER_SIZE
        buffer.limit(limit);
      }
    }
  }
}
