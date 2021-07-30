package org.pipecraft.pipes.sync.inter.join;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.inter.FilterPipe;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.SharderByHashPipe;
import org.pipecraft.pipes.utils.PipeSupplier;

/**
 * A pipe performing a join operation between a 'left' pipe of type L, and a list of 'right' pipes of type R.
 * In contrast to {@link SortedJoinPipe}, left and right pipes don't have to be ordered. 
 * This pipe uses a grace-hash-join approach, and requires the caller to be careful with the data partitioning
 * definitions, in order to prevent OOM errors.
 * 
 * Duplicates are allowed.
 * The output type for this pipe is {@link JoinRecord}, which consists of the key, the left matches and the right matches.
 * 
 * The join can work in LEFT/INNER/FULL_INNER/OUTER mode. See {@link JoinMode} for more details.
 * 
 * @param <K> The type of the key used for matching records
 * @param <L> The type of left side records
 * @param <R> The type of right side records
 *
 * @author Eyal Schneider
 */
public class HashJoinPipe <K, L, R> extends CompoundPipe<JoinRecord<K, L, R>> {
  private final Pipe<L> leftPipe;
  private final FailableFunction<L, K, PipeException> leftKeyExtractor;
  private final List<? extends Pipe<R>> rightPipes;
  private final FailableFunction<R, K, PipeException> rightKeyExtractor;
  private final JoinMode joinMode;
  private final CodecFactory<L> leftCodec;
  private final CodecFactory<R> rightCodec;
  private final File tmpFolder;
  private final int partitionCount;
  private File partitionsFolder;

  /**
   * Constructor
   * 
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   * @param partitionCount The number of partitions every pipe should be split into. 
   * Assuming a good hash function on item keys, the caller can assume the partitions are more-less even in size.
   * In the worst case, the same partition of all pipes will be loaded into memory at the same time.
   * This number determines the amount of memory to be used, so it should be determined with caution.
   * @param leftCodec An encoder/decoder factory for items of left pipe. Used for intermediate storage needed for the hash join.
   * @param rightCodec An encoder/decoder factory for items of right pipe. Used for intermediate storage needed for the hash join.
   * @param tmpFolder The folder to use for temporary storage of pipe contents
   */
  public HashJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor,
      JoinMode joinMode, int partitionCount, CodecFactory<L> leftCodec, CodecFactory<R> rightCodec, File tmpFolder) {
    this.leftPipe = leftPipe;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightPipes = rightPipes;
    this.rightKeyExtractor = rightKeyExtractor;
    this.joinMode = joinMode;
    this.partitionCount = partitionCount;
    this.leftCodec = leftCodec;
    this.rightCodec = rightCodec;
    this.tmpFolder = tmpFolder;
  }

  /**
   * Constructor
   * 
   * To be used when there's a single right pipe.
   * @param leftPipe The left side pipe in the join operation
   * @param leftKeyExtractor The extractor of the key from the data type of the left pipe
   * @param rightPipe The right side pipe 
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param joinMode The policy for performing the join. See {@link JoinMode}.
   * @param partitionCount The number of partitions every pipe should be split into. 
   * Assuming a good hash function on item keys, the caller can assume the partitions are more-less even in size.
   * In the worst case, the same partition of all pipes will be loaded into memory at the same time.
   * This number determines the amount of memory to be used, so it should be determined with caution.
   * @param leftCodec An encoder/decoder factory for items of left pipe. Used for intermediate storage needed for the hash join.
   * @param rightCodec An encoder/decoder factory for items of right pipes. Used for intermediate storage needed for the hash join.
   * @param tmpFolder The folder to use for temporary storage of pipe contents
   */
  public HashJoinPipe(Pipe<L> leftPipe, FailableFunction<L, K, PipeException> leftKeyExtractor, Pipe<R> rightPipe, FailableFunction<R, K, PipeException> rightKeyExtractor,
      JoinMode joinMode, int partitionCount, CodecFactory<L> leftCodec, CodecFactory<R> rightCodec, File tmpFolder) {
    this(leftPipe, leftKeyExtractor, Collections.singletonList(rightPipe), rightKeyExtractor, joinMode, partitionCount, leftCodec, rightCodec, tmpFolder);
  }

  /**
   * Constructor
   *
   * A constructor for the case of no left pipe. Assumes join type OUTER among the right pipes.
   * @param rightPipes The list of right side pipes. The order is important, and determines the ids given to the pipes in the iteration outputs (See {@link JoinRecord}).
   * @param rightKeyExtractor The extractor of the key from the data type of the right pipes
   * @param partitionCount The number of partitions every pipe should be split into. 
   * Assuming a good hash function on item keys, the caller can assume the partitions are more-less even in size.
   * In the worst case, the same partition of all pipes will be loaded into memory at the same time.
   * This number determines the amount of memory to be used, so it should be determined with caution.
   * @param rightCodec An encoder/decoder factory for items of right pipes. Used for intermediate storage needed for the hash join.
   * @param tmpFolder The folder to use for temporary storage of pipe contents
   */
  public HashJoinPipe(List<? extends Pipe<R>> rightPipes, FailableFunction<R, K, PipeException> rightKeyExtractor, int partitionCount, CodecFactory<R> rightCodec, File tmpFolder) {
    this(EmptyPipe.instance(), v -> null, rightPipes, rightKeyExtractor, JoinMode.OUTER, partitionCount, null, rightCodec, tmpFolder);
  }

  @Override
  protected Pipe<JoinRecord<K, L, R>> createPipeline() throws PipeException, InterruptedException {
    try {
      // Partition all pipes
      partitionsFolder = FileUtils.createTempFolder("hashjoin_shards", tmpFolder);
      partition(leftPipe, partitionsFolder, "L", leftKeyExtractor, leftCodec);
      for (int i = 0; i < rightPipes.size(); i++) {
        Pipe<R> rightPipe = rightPipes.get(i);
        partition(rightPipe, partitionsFolder, "R" + i, rightKeyExtractor, rightCodec);
      }
      
      // Build pipe suppliers for each partition
      List<PipeSupplier<JoinRecord<K, L, R>>> suppliers = new ArrayList<>(rightPipes.size() + 1);
      for (int partitionInd = 0; partitionInd < partitionCount; partitionInd++) {
        suppliers.add(getPartitionResultSupplier(partitionInd, partitionsFolder));
      }
      
      return new ConcatPipe<>(suppliers);
    } catch (IOException e) {
      throw new IOPipeException(e);
    } 
  }

  
  @Override
  public void close() throws IOException {
    super.close();
    FileUtils.deleteFiles(partitionsFolder);
  }

  private <Q> void partition(Pipe<Q> pipe, File outputFolder, String filenamePrefix, FailableFunction<Q, K, PipeException> keyExtractor, CodecFactory<Q> codec)
      throws IOException, PipeException, InterruptedException {
    String[] fileNames = new String[partitionCount];
    for (int i = 0; i < fileNames.length; i++) {
      fileNames[i] = getPartitionFilename(filenamePrefix, i);
    }
    try (SharderByHashPipe<Q> sharder = new SharderByHashPipe<>(pipe, codec, keyExtractor, ind -> fileNames[ind], partitionCount, outputFolder, new FileWriteOptions())) {
      sharder.start();
    }
  }
  
  private static String getPartitionFilename(String prefix, int index) {
    return prefix + "_" + index;
  }
  
  private PipeSupplier<JoinRecord<K, L, R>> getPartitionResultSupplier(int partitionInd, File partitionsFolder) {
    return () -> {
      HashMap<K, JoinRecord<K, L, R>> resultMap = new HashMap<>();
      
      // Build phase - Build map based on left pipe
      File file = new File(partitionsFolder, getPartitionFilename("L", partitionInd));
      if (file.exists()) { // The partitioner may have not encountered any data to place in this partition
        try (BinInputReaderPipe<L> leftReader = new BinInputReaderPipe<>(file, leftCodec)) {
          leftReader.start();
          
          L next;
          while ((next = leftReader.next()) != null) {
            K key = leftKeyExtractor.apply(next);
            JoinRecord<K, L, R> jr = resultMap.computeIfAbsent(key, k -> new JoinRecord<>(key));
            jr.addLeft(next);
            resultMap.put(key, jr);
          }        
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
      
      // probe phase - join with each of the right pipes, enriching the map with join info
      for (int rightPipeInd = 0; rightPipeInd < rightPipes.size(); rightPipeInd++) {
        file = new File(partitionsFolder, getPartitionFilename("R" + rightPipeInd, partitionInd));
        if (file.exists()) { // The partitioner may have not encountered any data to place in this partition
          try (BinInputReaderPipe<R> rightReader = new BinInputReaderPipe<>(file, rightCodec)) {
            rightReader.start();
            
            R next;
            while ((next = rightReader.next()) != null) {
              K key = rightKeyExtractor.apply(next);
              JoinRecord<K, L, R> jr = resultMap.get(key);
              if (jr == null) {
                if (joinMode == JoinMode.OUTER) {
                  jr = new JoinRecord<>(key);
                  jr.addRight(rightPipeInd, next);
                  resultMap.put(key, jr);
                }
              } else {
                jr.addRight(rightPipeInd, next);
              }
            }          
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
        }
      }

      CollectionReaderPipe<JoinRecord<K, L, R>> collectionReader = new CollectionReaderPipe<>(resultMap.values());
      return new FilterPipe<>(collectionReader, jr -> joinMode.shouldOutput(jr, rightPipes.size()));
    };
  }
}
