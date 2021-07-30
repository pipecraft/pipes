package org.pipecraft.infra.storage.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.SizedInputStream;

/**
 * A bucket implementation pointing to a local disk folder.
 * Intended mainly for test purposes.
 * 
 * Supported optional operations:
 * - File composing
 * - Exclusive file creation operation (lock functionality)
 * - Uploading data using OutputStream
 * - List-after-write consistency
 * 
 * Unsupported optional operations:
 * - Generating read/write signed URLs
 * - Public access for uploaded files
 * 
 * @author Eyal Schneider
 */
public class LocalDiskBucket extends Bucket<File> {
  private final File bucketFolder;
  
  /**
   * Constructor
   * 
   * @param baseFolder The folder under which the bucket is assumed to be located
   * @param bucketName
   */
  LocalDiskBucket(File baseFolder, String bucketName) {
    super(bucketName);
    this.bucketFolder = new File(baseFolder, bucketName);
  }
  
  @Override
  public void put(String key, InputStream input, long length, String contentType, boolean isPublic, boolean allowOverride) throws IOException {
    validateNotFolderPath(key);
    File file = getFileRefGenPath(key);

    File tmpFile = FileUtils.createTempFile("upload_tmp", ".tmp");
    try (
        BufferedInputStream is = FileUtils.getInputStream(input, new FileReadOptions());
        OutputStream os = new FileOutputStream(tmpFile)) {
      // Copy to tmp file
      IOUtils.copy(is, os, 64 * 1024);
      // Move file (hopefully atomic operation, not requiring a copy. This is highly dependent on the tmp folder
      // location and the file system being used
      if (!allowOverride) {
        Files.move(tmpFile.toPath(), file.toPath()); // Throws FileAlreadyExistsException if file exists
      } else {
        Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      tmpFile.delete();
    }
  }

  @Override
  public void copyToAnotherBucket(String fromKey, String toBucket, String toKey) throws IOException {
    File src = getFileRef(fromKey);
    File dst = getFileRefGenPath(toBucket, toKey);
    File tmpFile = FileUtils.createTempFile("copy_tmp", ".tmp");
    try {
      FileUtils.copyFile(src, tmpFile);
      // Hopefully an atomic operation. Depends on the temp folder location and file system. 
      Files.move(tmpFile.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING); 
    } finally {
      tmpFile.delete();
    }
  }

  @Override
  public void delete(File f) throws IOException {
    validateBelongsToBucket(f);
    
    if (!f.delete()) {
      throw new IOException("Failed deleting file " + f.getAbsolutePath());
    }
    
    // Delete empty folders all the way up, to conform with Bucket class contract
    File parent = f.getParentFile();
    while (!parent.equals(bucketFolder) && parent.list().length == 0) {
      FileUtils.deleteFiles(parent);
      parent = parent.getParentFile();
    }
  }

  @Override
  public void get(File meta, File output) throws IOException {
    validateBelongsToBucket(meta);
    FileUtils.copyFile(meta, output);
  }

  @Override
  public SizedInputStream getAsStream(File f, int chunkSize) throws IOException {
    validateBelongsToBucket(f);
    return new SizedInputStream(new FileInputStream(f), f.length());
  }

  @Override
  public OutputStream getOutputStream(String key, int chunkSize) throws IOException {
    validateNotFolderPath(key);
    File tmpFile = FileUtils.createTempFile("outputstream_tmp", ".tmp");
    return new AtomicFileCreatorOutputStream(tmpFile, getFileRefGenPath(key));
  }

  @Override
  public boolean exists(String key) throws IOException {
    File ref = getFileRef(key);
    return ref.isFile(); // should exist and be a file
  }

  @Override
  public Iterator<File> listObjects(String folderPath, boolean recursive) throws IOException {
    Path parent = getFileRef(folderPath).toPath();

    try {
      return Files.walk(parent, recursive ? Integer.MAX_VALUE : 1)
          .filter(p -> !p.equals(parent)) // Exclude root
          .map(Path::toFile)
          .iterator();
    } catch (NoSuchFileException e) {
      // Ignore and return an empty iterator, as required
      return Collections.emptyIterator();
    }
  }

  @Override
  public File getObjectMetadata(String key) throws IOException {
    File f = getFileRef(key);
    if (!f.exists() || f.isDirectory()) {
      throw new FileNotFoundException("File " + key + " not found in bucket " + getBucketName());
    }
    return f;
  }

  @Override
  public String getPath(File f) {
    try {
      String fullPath = f.getCanonicalPath();
      String bucketPath = bucketFolder.getCanonicalPath() + "/";
      if (!fullPath.startsWith(bucketPath)) {
        throw new IllegalArgumentException("Given file (" + f.getAbsolutePath() + ") doesn't belong to the bucket " + getBucketName());
      }
      
      return fullPath.substring(bucketPath.length()) + (f.isDirectory() ? "/" : "");
    } catch (IOException e) {
      throw new RuntimeException("Unable to canonize paths", e);
    }
  }

  @Override
  public long getLength(File f) {
    if (f.isDirectory()) {
      return 0;
    }
    return f.length();
  }

  @Override
  public Long getLastUpdated(File objMetadata) {
    if (objMetadata.isDirectory()) {
      return null;
    }
    return objMetadata.lastModified();
  }

  @Override
  public URL generateSignedUrl(String key, String contentType, int expirationSeconds,
      boolean isPublicRead) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generateReadOnlyUrl(String key, int expirationSeconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generateResumableSignedUrlForUpload(String key, String contentType,
      int expirationSeconds, Long maxContentLengthInBytes, boolean isPublic) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public File compose(List<String> paths, String composedFilePath, boolean removeComprisingFiles) throws IOException {
    File outFile = getFileRefGenPath(composedFilePath);
    if (outFile.isDirectory()) {
      throw new IOException("Illegal path: '" + composedFilePath + "'. Expected a file path.");
    }
    File tmpFile = FileUtils.createTempFile("compose_temp", ".tmp");
    try (OutputStream os = new FileOutputStream(tmpFile)) {
      for (String inputPath : paths) {
        File inFile = getFileRef(inputPath);
        try (InputStream is = new FileInputStream(inFile)) {
          IOUtils.copy(is, os, 50_000);
        }
      }
    }

    Files.move(tmpFile.toPath(), outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    
    // Cleanup
    if (removeComprisingFiles) {
      List<String> toDelete = paths.stream().filter(path -> !path.equals(composedFilePath)).collect(Collectors.toList()); // Avoid deleting the target file if it also appears as input
      try {
        deleteAllInterruptibly(toDelete, Runtime.getRuntime().availableProcessors());
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted while deleting compose source files");
      }
    }

    return outFile;
  }
  
  private File getFileRef(String path) {
    return new File(bucketFolder, path);
  }

  private File getFileRef(String bucket, String path) {
    return new File(new File(bucketFolder.getParent(), bucket), path);
  }

  private File getFileRefGenPath(String path) {
    return getFileRefGenPath(getBucketName(), path);
  }

  private File getFileRefGenPath(String bucket, String path) {
    File res = getFileRef(bucket, path);
    res.getParentFile().mkdirs();
    return res;
  }
  
  private void validateBelongsToBucket(File f) {
    getPath(f); // Throws IllegalArgumentException if f doesn't belong to the bucket
  }
  
  // A decorator writing to a temp file, and moving the final file "atomically" once the output stream is closed
  private static class AtomicFileCreatorOutputStream extends FilterOutputStream {
    private final File targetFile;
    private final File tmpFile;
    private boolean closed; 
    
    public AtomicFileCreatorOutputStream(File tmpFile, File targetFile) throws FileNotFoundException {
      super(new FileOutputStream(tmpFile));
      this.tmpFile = tmpFile;
      this.targetFile = targetFile;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        try {
          super.close();
          Files.move(tmpFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } finally {
          tmpFile.delete();
        }
      }
    }
  }
}
