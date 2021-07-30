package org.pipecraft.infra.storage.google_cs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.pipecraft.infra.storage.PathUtils;

/**
 * Adapter for Google Storage client. Loads the credentials on initialization
 * phase.
 *
 * @author Eyal Rubichi
 */
public class GoogleStorage implements org.pipecraft.infra.storage.Storage<GoogleStorageBucket, Blob> {
  private static final String GS_SCHEMA = "gs";

  private final Storage storage;

  /**
   * Google Storage Adapter constructor that uses environment variables. Connects
   * using the environment's credentials (in GOOGLE_APPLICATION_CREDENTIALS
   * environment variable). If the instance runs on Google Cloud, the environment
   * variable isn't needed.
   */
  public GoogleStorage() {
    this.storage = StorageOptions.getDefaultInstance().getService();
  }

  /**
   * returns a Google Storage bucket
   *
   * @param bucketName The required bucket name
   * @return The bucket object
   */
  public GoogleStorageBucket getBucket(String bucketName) {
    return new GoogleStorageBucket(storage, bucketName);
  }

  @Override
  public String getProtocol() {
    return GS_SCHEMA;
  }

  /**
   * Creates a fully qualified path consisting of parts that compose the path.
   * Bucket name must appear as the first part or as the prefix of the first part.
   *
   * @param parts ordered parts of the path (can be sub paths as well)
   * @return fully qualified GS path
   */
  public static String buildFullyQualifiedPath(String... parts) {
    return PathUtils.buildFullPath(GS_SCHEMA, parts);
  }
}
