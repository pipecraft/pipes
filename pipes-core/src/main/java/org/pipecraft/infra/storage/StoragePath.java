package org.pipecraft.infra.storage;

/**
 * A data class pointing to a specific protocol (e.g. gs/s3), bucket and path in some storage system.
 * 
 * @author Eyal Schneider
 */
public class StoragePath {
  private final String protocol;
  private final String bucket;
  private final String path;
  
  /**
   * Constructor
   * 
   * @param protocol The protocol (e.g. "s3")
   * @param bucket The bucket name
   * @param path The path under the given bucket
   */
  public StoragePath(String protocol, String bucket, String path) {
    this.protocol = protocol;
    this.bucket = bucket;
    this.path = path;
  }

  /**
   * @return The protocol (e.g. "s3")
   */
  public String getProtocol() {
    return protocol;
  }

  /**
   * @return The bucket name
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * @return The path under the given bucket
   */
  public String getPath() {
    return path;
  }

  
  @Override
  public String toString() {
    return PathUtils.buildFullPath(protocol, bucket, path);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    result = prime * result + ((protocol == null) ? 0 : protocol.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    StoragePath other = (StoragePath) obj;
    if (bucket == null) {
      if (other.bucket != null)
        return false;
    } else if (!bucket.equals(other.bucket))
      return false;
    if (path == null) {
      if (other.path != null)
        return false;
    } else if (!path.equals(other.path))
      return false;
    if (protocol == null) {
      if (other.protocol != null)
        return false;
    } else if (!protocol.equals(other.protocol))
      return false;
    return true;
  }
}
