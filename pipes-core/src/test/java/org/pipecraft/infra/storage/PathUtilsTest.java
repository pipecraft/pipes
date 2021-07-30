package org.pipecraft.infra.storage;



import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * @author Eyal Schneider, Eyal Rubichi
 */
public class PathUtilsTest {

  @Test
  public void testBuildPath() {
    assertEquals("abc", PathUtils.buildPath("abc"));
    assertEquals("abc/def/ghi", PathUtils.buildPath("abc", "def", "ghi"));
    assertEquals("abc/def/ghi", PathUtils.buildPath("abc", "def/ghi"));
    assertEquals("abc/def/ghi", PathUtils.buildPath("abc", "def///ghi"));
    assertEquals("abc/def/ghi", PathUtils.buildPath("abc/", "/def/", "/ghi"));
    assertEquals("abc/def/ghi", PathUtils.buildPath("", "abc/", "/def/", "/ghi"));
  }

  @Test
  public void testGSBuildFullyQualifiedPath() {
    assertEquals("gs://abc", PathUtils.buildFullPath("gs", "abc"));
    assertEquals("gs://abc/def/ghi", PathUtils.buildFullPath("gs", "abc", "def", "ghi"));
    assertEquals("gs://abc/def/ghi", PathUtils.buildFullPath("gs", "abc", "def/ghi"));
    assertEquals("s3://abc/def/ghi", PathUtils.buildFullPath("s3", "abc", "def///ghi"));
    assertEquals("gs://abc/def/ghi", PathUtils.buildFullPath("gs", "abc/", "/def/", "/ghi"));
  }

  @Test
  public void getLastPathPart() {
    assertEquals("abc", PathUtils.getLastPathPart("abc"));
    assertEquals("abc", PathUtils.getLastPathPart("abc/"));
    assertEquals("abc", PathUtils.getLastPathPart("aa/abc"));
    assertEquals("abc", PathUtils.getLastPathPart("/aa/abc/"));
    assertEquals("de.F", PathUtils.getLastPathPart("abc/de.F"));
    assertEquals("de.f", PathUtils.getLastPathPart("abc/de.f/"));
  }

  @Test
  public void getProtocol() {
    assertEquals("s3", PathUtils.getProtocolFromFullyQualifiedPath("s3://my.bucket./a/b/c"));
    assertEquals("gs", PathUtils.getProtocolFromFullyQualifiedPath("gs://mybucket123/a/b/c"));
    assertEquals("ab", PathUtils.getProtocolFromFullyQualifiedPath("ab://mybucket123/a/b/c"));
    assertNull(PathUtils.getProtocolFromFullyQualifiedPath("mybucket123/a/b/c"));
  }

  @Test
  public void getBucketName() {
    assertEquals("my-bucket", PathUtils.getBucketNameFromFullyQualifiedPath("gs://my-bucket/a/b/c"));
    assertEquals("my_bucket", PathUtils.getBucketNameFromFullyQualifiedPath("gs://my_bucket/a/b/C"));
    assertEquals("my.bucket.", PathUtils.getBucketNameFromFullyQualifiedPath("gs://my.bucket./a/b/c"));
    assertEquals("mybucket123", PathUtils.getBucketNameFromFullyQualifiedPath("gs://mybucket123/a/b/c"));
    assertNull(PathUtils.getBucketNameFromFullyQualifiedPath("a/b/myBucket123/a/b/c"));
  }

  @Test
  public void getKey() {
    assertEquals("a/b/c-d_", PathUtils.getKeyFromFullyQualifiedPath("gs://my-bucket/a/b/c-d_"));
    assertEquals("", PathUtils.getKeyFromFullyQualifiedPath("gs://my-bucket/"));
    assertEquals("A.txt", PathUtils.getKeyFromFullyQualifiedPath("gs://my-bucket/A.txt"));
  }
  
  @Test
  public void parsePathTest() {
    assertEquals(new StoragePath("gs", "my-bucket", "a/b/c"), PathUtils.parseFullyQualifiedPath("gs://my-bucket/a/b/c"));
    assertEquals(new StoragePath("s3", "my_bucket", "a/b/c/"), PathUtils.parseFullyQualifiedPath("s3://my_bucket/a/b/c/"));
    assertEquals(new StoragePath("file", "my_bucket", "a/b/c/file.txt"), PathUtils.parseFullyQualifiedPath("file://my_bucket/a/b/c/file.txt"));
    assertNull(PathUtils.parseFullyQualifiedPath("gs:/mybucket123/a"));
  }

}
