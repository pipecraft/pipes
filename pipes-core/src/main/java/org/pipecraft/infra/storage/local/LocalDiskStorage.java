package org.pipecraft.infra.storage.local;

import java.io.File;

import org.pipecraft.infra.storage.Storage;

/**
 * A storage implementation for working on local disk.
 * Useful mainly for tests.
 * 
 * @author Eyal Schneider
 */
public class LocalDiskStorage implements Storage<LocalDiskBucket, File> {
  private final File baseStorageFolder;

  /**
   * Constructor
   *
   * @param baseStorageFolder The local folder under all bucket folders are expected to be located
   */
  public LocalDiskStorage(File baseStorageFolder) {
    this.baseStorageFolder = baseStorageFolder;
  }
  
  @Override
  public LocalDiskBucket getBucket(String bucketName) {
    return new LocalDiskBucket(baseStorageFolder, bucketName);
  }

  @Override
  public String getProtocol() {
    return "file";
  }

  /**
   * Creates the bucket if not exists already, and returns the object for manipulating it
   * @param bucketName The name of the bucket
   * @return The bucket object, pointing to the existing / new bucket
   */
  public LocalDiskBucket getOrCreateBucket(String bucketName) {
    File bucketFolder = new File(baseStorageFolder, bucketName);
    bucketFolder.mkdirs();
    if (!bucketFolder.isDirectory()) {
      throw new IllegalArgumentException("The bucket folder ('" + bucketFolder + "') can't be created or exists and isn't a folder");
    }
    return new LocalDiskBucket(baseStorageFolder, bucketName);
  }
}
