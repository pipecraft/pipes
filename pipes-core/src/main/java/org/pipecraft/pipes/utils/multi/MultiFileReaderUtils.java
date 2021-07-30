package org.pipecraft.pipes.utils.multi;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import org.pipecraft.infra.math.ArithmeticUtils;
import org.pipecraft.infra.math.StaticJobScheduler;
import org.pipecraft.infra.storage.Bucket;
import org.pipecraft.pipes.sync.inter.CallbackPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.utils.ShardSpecifier;

/**
 * Shared helper methods for multi-file reader pipes.
 * 
 * @author Eyal Schneider
 */
public class MultiFileReaderUtils {
  private static final Logger logger = LoggerFactory.getLogger(MultiFileReaderUtils.class);

  /**
   * @param filePath File path to check against the given shard
   * @param shardSpecifier The shard
   * @return true if and only if the file belongs to the given shard.
   * Shards are determined by hashing of the file paths.
   */
  public static boolean isOwned(String filePath, ShardSpecifier shardSpecifier) {
    byte[] pathAsBin = filePath.getBytes(StandardCharsets.UTF_8);
    return ArithmeticUtils.getShardByStrongHash(pathAsBin, shardSpecifier.getShardCount()) == shardSpecifier.getShardIndex();
  }
  
  /**
   * @param config The configuration of the multi file reader, as {@link LocalMultiFileReaderConfig}
   * @return the set of local files that the reader should read, according to the locations and filters
   * specified in the config
   * @throws IOException If an IO error occures while listing local files
   */
  public static <T> Collection<File> getAllLocalFilesToRead(LocalMultiFileReaderConfig<T> config) throws IOException {
    Collection<File> filesToRead;
    if (config.isBalancedSharding()) { // Auto sharding with balancing
      ShardSpecifier shard = config.getShardSpecifier();
      Collection<File> allFiles = filterLocalFiles(config.getPaths(), config.isRecursivePaths(), config.getFileFilter());
      StaticJobScheduler<File> balancer = new StaticJobScheduler<>(allFiles, f -> (double)f.length());
      List<Collection<File>> filePartition = balancer.schedule(shard.getShardCount());
      filesToRead = filePartition.get(shard.getShardIndex());
    } else {
      Predicate<File> filePredicate = config.getFileFilter();
      if (config.getShardSpecifier() != null) {
        filePredicate = filePredicate.and(f -> MultiFileReaderUtils.isOwned(f.getAbsolutePath(), config.getShardSpecifier()));
      }
      filesToRead = filterLocalFiles(config.getPaths(), config.isRecursivePaths(), filePredicate);
    }
    long totalVolumeBytes = filesToRead.stream().map(File::length).mapToLong(x -> x).sum();
    logger.debug("Current pipe is about to read " + filesToRead.size() + " files, totalling in " + totalVolumeBytes + " bytes");
    return filesToRead;
  }

  /**
   * @param config The configuration of the multi file reader, as {@link LocalMultiFileReaderConfig}
   * @return the set of remote files that the reader should read, according to the locations and filters
   * specified in the config
   * @throws IOException If an IO error occures while listing remote files
   */
  public static <T, B> Collection<B> getAllRemoteFilesToRead(StorageMultiFileReaderConfig<T, B> config) throws IOException {
    Collection<B> filesToRead;
    if (config.isBalancedSharding()) { // Auto sharding with balancing
      ShardSpecifier shard = config.getShardSpecifier();
      Collection<B> allFiles = filterRemoteFiles(config.getBucket(), config.getPaths(), config.isRecursivePaths(), config.getFileFilter());
      StaticJobScheduler<B> balancer = new StaticJobScheduler<>(allFiles, f -> (double)config.getBucket().getLength(f));
      List<Collection<B>> filePartition = balancer.schedule(shard.getShardCount());
      filesToRead = filePartition.get(shard.getShardIndex());
    } else {
      Predicate<B> filePredicate = config.getFileFilter();
      if (config.getShardSpecifier() != null) {
        Bucket<B> bucket = config.getBucket();
        filePredicate = filePredicate.and(f -> MultiFileReaderUtils.isOwned(bucket.getPath(f), config.getShardSpecifier()));
      }
      filesToRead = filterRemoteFiles(config.getBucket(), config.getPaths(), config.isRecursivePaths(), filePredicate);
    }
    long totalVolumeBytes = filesToRead.stream().map(config.getBucket()::getLength).mapToLong(x -> x).sum();
    logger.debug("Current pipe is about to read " + filesToRead.size() + " remote files, totalling in " + totalVolumeBytes + " bytes");
    return filesToRead;
  }

  public static <T, B> List<PipeSupplier<T>> downloadAndGetReadPipes(Collection<B> files, Bucket<B> bucket,
      StorageMultiFileReaderConfig<T, B> config) throws IOException, InterruptedException {
   List<PipeSupplier<T>> pipeSuppliers = new ArrayList<>(files.size());
   
   Map<String, String> remotePathToLocalName = new HashMap<>(); // Maps remote file path to local file name
   for (B file : files) {
     String remotePath = bucket.getPath(file);
     String localPath = remotePath.replaceAll("/", "__");
     remotePathToLocalName.put(remotePath, localPath);
   }
   File downloadFolder = FileUtils.createTempFolder("download", config.getTmpFolder());
   logger.debug("Downloading " + files.size() + " files...");
   bucket.getAllRegularFilesByMetaInterruptibly(files, downloadFolder, remotePathToLocalName::get, config.getThreadNum());
   logger.debug("Done downloading " + files.size() + " files.");
   
   for (B remoteFile : files) {
     String remotePath = bucket.getPath(remoteFile);
     String localPath = remotePathToLocalName.get(remotePath);
     File f = new File(downloadFolder, localPath);
     pipeSuppliers.add(() -> {
       Pipe<T> input = config.getPipeSupplier().get(new SizedInputStream(new FileInputStream(f), f.length()), remoteFile);
       return new CallbackPipe<>(input, f::delete);
     });
   }

   return pipeSuppliers;
 }

  private static Collection<File> filterLocalFiles(Collection<String> folderPaths, boolean isRecursive, Predicate<File> filePathPredicate) throws IOException {
    try {
      Collection<File> res = new ArrayList<>();
      for (String folderPath : folderPaths) {
        File folder = new File(folderPath);
        if (!folder.isDirectory()) {
          throw new IOException("'" + folder.getAbsolutePath() + "' isn't a folder or can't be read");
        }
      
        Path folderP = folder.toPath();
        Files.walk(folderP, isRecursive ? Integer.MAX_VALUE : 1)
            .map(Path::toFile)    
            .filter(f -> !f.isDirectory()) // Exclude all folders
            .forEach(f -> {        
              if (filePathPredicate.test(f)) {
                res.add(f);
              }
            });
      }
      return res;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private static <B> Collection<B> filterRemoteFiles(Bucket<B> bucket, Collection<String> folderPaths, boolean isRecursive, Predicate<B> filePathPredicate) throws IOException {
    try {
      Collection<B> res = new ArrayList<>();
      for (String folderPath : folderPaths) {
        Iterator<B> it = isRecursive ? bucket.listFilesRecursive(folderPath) : bucket.listFiles(folderPath);
        while (it.hasNext()) {
          B blob = it.next();
          if (filePathPredicate.test(blob)) {
            res.add(blob);
          }
        }
      }
      return res;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
