package org.pipecraft.infra.storage;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A set of utility methods for working with storage paths
 * 
 * @author Eyal Schneider
 */
public class PathUtils {
  private static final String PROTOCOL_SUFFIX = "://";
  private static final String PATH_SEPARATOR = "/";
  private static final Pattern FULLY_QUALIFIED_PATH_PATTERN = Pattern.compile("(?<protocol>[a-zA-Z0-9]*)://(?<bucketName>[a-z0-9_\\-.]+)/(?<key>.*)");

  /**
   * Builds a path from the given parts (not including protocol)
   * Handles normalization of the path by eliminating consecutive separators, using separators between paths. and 
   * removing leading separators if needed.
   *
   * @param parts path parts. May be single path parts or sub paths
   * @return a single path composed by the given parts
   */
  public static String buildPath(String... parts) {
    return buildPath(false, parts);
  }

  /**
   * Builds a full cloud path from the given parts.
   * Handles normalization of the path by eliminating consecutive separators, using separators between paths. and 
   * removing leading separators if needed.
   *
   * @param protocol The protocol name (e.g. "gs") with no separators
   * @param parts path parts. May be single path parts or sub paths
   * @return a single path composed by the given parts
   */
  public static String buildFullPath(String protocol, String... parts) {
    return protocol + PROTOCOL_SUFFIX + buildPath(false, parts);
  }

  /**
   * Builds a cloud path from the given parts (not including protocol)
   * Handles normalization of the path by eliminating consecutive separators, using separators between paths. and 
   * removing leading separators if needed.
   *
   * @param isFolder indicates whether the path should lead to a folder
   * @param parts path parts. May be single path parts or sub paths
   * @return a single path composed by the given parts
   */
  public static String buildPath(boolean isFolder, String... parts) {
    String path = String.join(PATH_SEPARATOR, parts)
        .concat(isFolder ? PATH_SEPARATOR : "")
        .replaceAll(PATH_SEPARATOR + "{2,}", PATH_SEPARATOR); // eliminate consecutive separators

    if (path.startsWith(PATH_SEPARATOR)) {
      path = path.substring(1); // remove leading separator
    }

    return path;
  }

  /**
   * Builds a full cloud path from the given parts.
   * Handles normalization of the path by eliminating consecutive separators, using separators between paths. and 
   * removing leading separators if needed.
   * 
   * @param protocol The protocol name (e.g. "gs") with no separators
   * @param isFolder indicates whether the path should lead to a folder
   * @param parts path parts. May be single path parts or sub paths
   * @return a single path composed by the given parts
   */
  public static String buildPath(String protocol, boolean isFolder, String... parts) {
    return protocol + PROTOCOL_SUFFIX + buildPath(isFolder, parts);
  }

  /**
   * Returns the last part of a given path.
   * For example:
   * 1. if the path is `/aa/bb/cc.json` the returned value would be `cc.json`
   * s. if the path is `/aa/bb/cc` the returned value would be `cc`
   *
   * @param path cloud storage path
   * @return a single path composed by the given parts
   */
  public static String getLastPathPart(String path) {
    String[] pathParts = path.split(PATH_SEPARATOR);
    return pathParts[pathParts.length - 1];
  }

  /**
   * @param path a cloud storage path to extract the parent from. Assumed to include at least one path separator.
   * @return the parent path
   */
  public static String getParentPath(String path) {
    return path.substring(0, path.lastIndexOf(PATH_SEPARATOR) + 1);
  }


  /**
   * Gets a fully qualified path ({protocol}://{bucket}/{path}) and returns
   * the protocol name (e.g. "gs" or "s3")
   * @param fullyQualifiedPath The path to examine
   * @return protocol or null if it doesn't exist
   */
  public static String getProtocolFromFullyQualifiedPath(String fullyQualifiedPath) {
    Matcher matcher = FULLY_QUALIFIED_PATH_PATTERN.matcher(fullyQualifiedPath);
    if (matcher.matches()) {
      return matcher.group("protocol");
    }
    return null;
  }

  /**
   * Gets a fully qualified path ({protocol}://{bucket}/{path}) and returns
   * the bucket name.
   * @param fullyQualifiedPath The path to examine
   * @return bucket name or null if it doesn't exist
   */
  public static String getBucketNameFromFullyQualifiedPath(String fullyQualifiedPath) {
    Matcher matcher = FULLY_QUALIFIED_PATH_PATTERN.matcher(fullyQualifiedPath);
    if (matcher.matches()) {
      return matcher.group("bucketName");
    }
    return null;
  }

  /**
   * Gets a fully qualified path ({protocol}://{bucket}/{path}) and returns
   * the key.
   * @param fullyQualifiedPath The fully qualified path ({protocol}://{bucket}/{path})
   * @return key path or null if it doesn't exist
   */
  public static String getKeyFromFullyQualifiedPath(String fullyQualifiedPath) {
    Matcher matcher = FULLY_QUALIFIED_PATH_PATTERN.matcher(fullyQualifiedPath);
    if (matcher.matches()) {
      return matcher.group("key");
    }
    return null;
  }

  /**
   * Parses a fully qualified path and returns the protocol+bucket+key triplet.
   * @param fullyQualifiedPath The fully qualified path ({protocol}://{bucket}/{path})
   * @return A the storage path object, containing the 3 parsed path components
   */
  public static StoragePath parseFullyQualifiedPath(String fullyQualifiedPath) {
    Matcher matcher = FULLY_QUALIFIED_PATH_PATTERN.matcher(fullyQualifiedPath);    
    if (matcher.matches()) {
      return new StoragePath(matcher.group("protocol"), matcher.group("bucketName"), matcher.group("key"));
    }
    return null;
  }
}
