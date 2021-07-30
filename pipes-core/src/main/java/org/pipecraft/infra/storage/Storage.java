package org.pipecraft.infra.storage;

/**
 * A parent interface for all remote/local storage implementations
 * 
 * @author Eyal Schneider
 *
 * @param <S> The bucket class type
 * @param <T> The object metadata type
 */
public interface Storage<S extends Bucket<T>, T> {
  
  /**
   * @param bucketName The bucket name
   * @return The bucket object for performing file operations on the given bucket. Calling this method on a non existing bucket should
   * not fail and should not have any side effects.
   */
  S getBucket(String bucketName);

  /**
   * @return The protocol name serving as a prefix when using fully qualified paths (e.g. "gs" or "s3")
   */
  String getProtocol();
}
