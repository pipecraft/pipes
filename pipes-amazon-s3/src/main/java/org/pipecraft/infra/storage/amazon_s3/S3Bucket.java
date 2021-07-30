package org.pipecraft.infra.storage.amazon_s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kms.model.KeyUnavailableException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Iterators;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.storage.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage bucket implementation based on Amazon's S3.
 * 
 * Supports the optional operation of uploading files with public access, 
 * but doesn't support the following optional {@link Bucket} operations:
 * - Generating read/write signed URLs
 * - Composing remote files
 * - Exclusive file creation operation (lock functionality)
 * - Uploading data using OutputStream
 * 
 * In addition S3 doesn't support List-after-write consistency.
 *
 * @author Ben Bonfil , Eyal Schneider
 */
public class S3Bucket extends Bucket<S3ObjectSummary> {

  /**
   * S3 connection
   */
  private final AmazonS3 s3;

  private static final Logger logger = LoggerFactory.getLogger(S3Bucket.class);

  /**
   * Constructor
   *
   * @param connection AmazonS3 connection
   * @param bucketName bucket name
   */
  S3Bucket(AmazonS3 connection, String bucketName) {
    super(bucketName);
    s3 = connection;
  }

  public void put(String key, InputStream input, long length, String contentType, boolean isPublic, boolean allowOverride) throws IOException {
    if (!allowOverride) {
      throw new UnsupportedOperationException();
    }

    validateNotFolderPath(key);
    
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(length);
    meta.setContentType(contentType);

    PutObjectRequest putRequest = new PutObjectRequest(getBucketName(), key, input, meta);
    if (isPublic) {
      putRequest.setCannedAcl(CannedAccessControlList.PublicRead);
    }
    try {
      s3.putObject(putRequest);
    } catch (AmazonClientException e) {
      throw mapToIOException(extractAmazonServiceException(e), "Failed uploading '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public OutputStream getOutputStream(String key, int chunkSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyToAnotherBucket(String fromKey, String toBucket, String toKey) throws IOException {
    try {
      CopyObjectRequest copyObjRequest = new CopyObjectRequest(
          getBucketName(), fromKey, toBucket, toKey);
      s3.copyObject(copyObjRequest);
    } catch (AmazonClientException e) {
      throw mapToIOException(extractAmazonServiceException(e), "Failed copying '" + getBucketName() + "/" + fromKey + "' to " + "'" + toBucket + "/" + toKey + "'");
    }
  }

  @Override
  public void delete(S3ObjectSummary obj) throws IOException {
    try {
      DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(getBucketName(), obj.getKey());
      s3.deleteObject(deleteObjectRequest);
    } catch (AmazonClientException e) {
      throw mapToIOException(extractAmazonServiceException(e), "Failed deleting '" + getBucketName() + "/" + obj.getKey() + "'");
    }
  }

  public void get(S3ObjectSummary meta, File output) throws IOException {
    try {
      GetObjectRequest req = new GetObjectRequest(getBucketName(), meta.getKey());
      s3.getObject(req, output);
    } catch (KeyUnavailableException e) {
      throw new FileNotFoundException("Key " + meta.getKey() + " not found in bucket " + getBucketName());
    } catch (AmazonClientException e) {
      if (!output.delete()) {   // file which wasn't fully retrieved must be deleted.
        logger.error("Cannot delete the malformed file from a failed download.");
      }
      throw mapToIOException(extractAmazonServiceException(e), "Failed downloading '" + getBucketName() + "/" + meta.getKey() + "'");
    }
  }

  @Override
  public SizedInputStream getAsStream(S3ObjectSummary meta, int chunkSize) throws IOException {
    GetObjectRequest req = new GetObjectRequest(getBucketName(), meta.getKey());
    S3Object obj;
    try {
      obj = s3.getObject(req);
    } catch (AmazonClientException ace) {
      throw mapToIOException(extractAmazonServiceException(ace), "Failed getting input stream for '" + getBucketName() + "/" + meta.getKey() + "'");
    }
    return new SizedInputStream(obj.getObjectContent(), obj.getObjectMetadata().getContentLength());
  }

  @Override
  public Iterator<S3ObjectSummary> listObjects(String folderPath, boolean recursive) throws IOException {
    String normalizedPath = normalizeFolderPath(folderPath);
    return Iterators.filter(new S3ObjectIterator(normalizedPath, recursive), 
        v -> !v.getKey().equals(normalizedPath)); // Filtering of the folder itself, which is present if it was created independently as an object!
  }


  @Override
  public URL generateSignedUrl(String key, String contentType, int expirationSeconds, boolean isPublicRead) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generateReadOnlyUrl(String key, int expirationSeconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generateResumableSignedUrlForUpload(String key, String contentType, int expirationSeconds, Long maxContentLengthInBytes, boolean isPublic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public S3ObjectSummary getObjectMetadata(String key) throws IOException {
    if (isFolderPath(key)) {
      throw new FileNotFoundException("File not found: '" + getBucketName() + "/" + key + "'");
    }
    try {
      ObjectMetadata meta = s3.getObjectMetadata(getBucketName(), key);
      S3ObjectSummary res = new S3ObjectSummary();
      res.setBucketName(getBucketName());
      res.setKey(key);
      res.setETag(meta.getETag());
      res.setLastModified(meta.getLastModified());
      res.setSize(meta.getContentLength());
      res.setStorageClass(meta.getStorageClass());
      return res;
    } catch (AmazonClientException ace) {
      throw mapToIOException(extractAmazonServiceException(ace), "Failed getting metadata of '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public String getPath(S3ObjectSummary keyMetadata) {
    if (!keyMetadata.getBucketName().equals(getBucketName())) {
      throw new IllegalArgumentException("The given meta object belongs to a different bucket ('" + keyMetadata.getBucketName() + "')");
    }
    return keyMetadata.getKey();
  }

  @Override
  public long getLength(S3ObjectSummary keyMetadata) {
    return keyMetadata.getSize();
  }

  @Override
  public Long getLastUpdated(S3ObjectSummary objMetadata) {
    Date lastModified = objMetadata.getLastModified();
    return lastModified == null ? null : lastModified.getTime();
  }

  @Override
  public boolean exists(String key) throws IOException {
    try {
      return s3.doesObjectExist(getBucketName(), key) && isFilePath(key);
    } catch (AmazonClientException ace) {
      throw mapToIOException(extractAmazonServiceException(ace), "Failed checking existence of '" + getBucketName() + "/" + key + "'");
    }
  }

  @Override
  public S3ObjectSummary compose(List<String> gsPaths, String composedFilePath, boolean removeComprisingFiles) throws IOException {
    throw new UnsupportedOperationException();
  }  
    
  // Scans the exception chain, and looks for the first AmazonServiceException under it.
  // Returns null if not found
  private static AmazonServiceException extractAmazonServiceException(Throwable e) {
    while (!(e instanceof AmazonServiceException)) {
      e = e.getCause();
      if (e == null) {
        return null;
      }
    }
    return (AmazonServiceException) e;
  }
  
  // Converts a AmazonServiceException to an IOException, trying to use a specific one if possible.
  // Accepts nulls.
  private static IOException mapToIOException(AmazonServiceException e, String msg) {
    if (e == null) {
      return new IOException(msg);
    }
    switch (e.getErrorCode()) {
      case "404 Not Found": return new FileNotFoundException(msg + ". Remote file not found.");
    }
    return new IOException(msg, e);
  }

  // An iterator on a file listing in s3. 
  // In case of non-recursive listing, handles differently "folders", as required by s3 API,
  // and streams them back as if they were standard objects
  private class S3ObjectIterator implements Iterator<S3ObjectSummary> {
    private ObjectListing page;
    private ListObjectsRequest req;
    private List<S3ObjectSummary> currList; 
    private int currIndexInList;
    
    public S3ObjectIterator(String prefix, boolean recursive) throws IOException {
      try {
        req = new ListObjectsRequest()
            .withBucketName(getBucketName()).withPrefix(prefix);
        if (!recursive) {
          req.withDelimiter(S3.S3_PATH_SEPARATOR);
        }
        this.page = s3.listObjects(req);
        this.currList = buildList(page);
      } catch (AmazonClientException ace) {
        throw mapToIOException(extractAmazonServiceException(ace), "");
      }
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (page.getNextMarker() == null && currIndexInList >= currList.size()) {
          return false;
        }
        
        if (currIndexInList < currList.size()) {
          return true;
        }
        
        // If we got here we must try fetching next page in order to know whether we have a next item
        fetchNextPage();
      }
    }

    @Override
    public S3ObjectSummary next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (currList.size() == currIndexInList) {
        fetchNextPage();
      }
      return currList.get(currIndexInList++);
    }

    private void fetchNextPage() {
      try {
        req.setMarker(page.getNextMarker());
        this.page = s3.listObjects(req);
        this.currList = buildList(page);
        this.currIndexInList = 0;
      } catch (AmazonClientException e) {
        throw new UncheckedIOException(mapToIOException(extractAmazonServiceException(e), "IO failure during remote files iteration"));
      }
    }
    
    // Assembles file objects and folder objects
    private List<S3ObjectSummary> buildList(ObjectListing page) {
      List<S3ObjectSummary> fileSummaries = page.getObjectSummaries();
      List<S3ObjectSummary> res = new ArrayList<>(fileSummaries.size() + page.getCommonPrefixes().size());
      res.addAll(fileSummaries);
      for (String subFolderPath : page.getCommonPrefixes()) {
        S3ObjectSummary subFolderSummary = new S3ObjectSummary();
        subFolderSummary.setBucketName(getBucketName());
        subFolderSummary.setKey(subFolderPath);
        res.add(subFolderSummary);
      }
      return res;
    }
  }
}
