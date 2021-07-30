package org.pipecraft.infra.storage.amazon_s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.pipecraft.infra.storage.Storage;

/**
 * A storage implementation for S3 service.
 * Note that this implementation always uses region US_EAST_1.
 *
 * @author Eyal Schneider
 */
public class S3 implements Storage<S3Bucket, S3ObjectSummary> {

  public static final String S3_PATH_SEPARATOR = "/";

  private final AmazonS3 impl;

  /**
   * Constructor
   *
   * @param credentials The AWS credentials. Use null for environment's credentials.
   */
  public S3(AWSCredentials credentials) {
    this.impl = connect(credentials);
  }

  /**
   * Constructor
   *
   * Uses environment credentials
   */
  public S3() {
    this(null);
  }

  @Override
  public S3Bucket getBucket(String bucketName) {
    return new S3Bucket(impl, bucketName);
  }

  @Override
  public String getProtocol() {
    return "s3";
  }

  /**
   * S3Bucket factory
   *
   * @param credentials The AWS credentials. Use null for environment's credentials.
   * @param name The required bucket name
   * @return The bucket object
   */
  public static S3Bucket bucket(AWSCredentials credentials, String name) {
    return new S3Bucket(S3.connect(credentials), name);
  }

  /**
   * @param credentials The AWS credentials. Use null for environment's credentials.
   * @return a new S3 object. Connects using the given credentials.
   */
  private static AmazonS3 connect(AWSCredentials credentials) {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1);
    if (credentials != null) {
      builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
    }
    return builder.build();
  }
}
