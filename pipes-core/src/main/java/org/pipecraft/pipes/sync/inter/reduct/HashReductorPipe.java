package org.pipecraft.pipes.sync.inter.reduct;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.terminal.SharderByHashPipe;
import org.pipecraft.pipes.utils.PipeSupplier;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Scans the input pipe and performs a reduction operation on families of items based on some discriminating property.
 * The discrimination logic and reduction logic are provided by the caller.
 * Unlike {@link SequenceReductorPipe} the input need not be sorted in order for it to work properly.
 * This pipe makes use of temporary disk space.
 * The configuration of partitionCount is critical for bounding memory usage. The more partitions are used, the less memory is required.
 * It's recommended to set this number to {estimated total input data volume} / {max memory allowed for this pipe to use}.
 *
 * @param <I> The data type of items in the input pipe
 * @param <O> The data type of output items
 *
 * @author Eyal Schneider
 */
public class HashReductorPipe<I, O> extends CompoundPipe<O> {

  private final Pipe<I> input;
  private final CodecFactory<I> inputCodec;
  private final int partitionCount;
  private final ReductorConfig<I, Object, Object, O> reductorConfig;
  private final File tmpFolder;
  private File tmpSubFolder;

  /**
   * Constructor
   *
   * @param input The input pipe to wrap
   * @param inputCodec A codec allowing writing/reading input records
   * @param partitionCount The number of partitions to split input into.
   * Assuming a good hash function on item keys, and assuming that the families defined by the discriminator are even in size, the caller can assume the partitions are more-less balanced in size.
   * This number determines the amount of memory to be used, so it should be defined with caution. The more partitions are used, the less total memory is
   * required. However, note that for each partition the class maintains an open file on disk.
   * @param tmpFolder The folder where to store temporary data
   * @param reductorConfig The reduction configuration
   */
  @SuppressWarnings("unchecked")
  public HashReductorPipe(Pipe<I> input, CodecFactory<I> inputCodec, int partitionCount, File tmpFolder, ReductorConfig<I, ?, ?, O> reductorConfig) {
    this.input = input;
    this.inputCodec = inputCodec;
    this.partitionCount = partitionCount;
    this.tmpFolder = tmpFolder;
    this.reductorConfig = (ReductorConfig<I, Object, Object, O>) reductorConfig;
  }

  @Override
  protected Pipe<O> createPipeline() throws PipeException, InterruptedException {
    tmpSubFolder = null;
    try {
      // Split input into shards (keeping families together in the same shard)
      tmpSubFolder = FileUtils.createTempFolder("hashReductor", tmpFolder);
      try (
          SharderByHashPipe<I> sharder = new SharderByHashPipe<>(input, inputCodec,
              reductorConfig.getDiscriminator(), String::valueOf, partitionCount, tmpSubFolder, new FileWriteOptions().temp(true))) {
        sharder.start();
      }

      // Build pipes that read the shards one by one and produce outputs
      List<PipeSupplier<O>> outputPipes = new ArrayList<>();
      for (int shard = 0; shard < partitionCount; shard++) {
        File file = new File(tmpSubFolder, String.valueOf(shard));
        if (file.exists()) {
          PipeSupplier<O> supplier = () -> {
            try (BinInputReaderPipe<I> fileReaderP = new BinInputReaderPipe<>(file, inputCodec)) {
              fileReaderP.start();
              HashMap<Object, Object> map = new HashMap<>();
              I item;
              while ((item = fileReaderP.next()) != null) {
                Object family = reductorConfig.getDiscriminator().apply(item);
                Object intermediate = map.computeIfAbsent(family, k -> reductorConfig.getAggregatorCreator().apply(family));
                reductorConfig.getAggregationLogic().accept(intermediate, item);
              }
              List<O> shardOutput = new ArrayList<>();
              for (Object intermediate : map.values()) {
                shardOutput.add(reductorConfig.getPostProcessor().apply(intermediate));
              }
              return new CollectionReaderPipe<>(shardOutput);
            }
          };
          outputPipes.add(supplier);
        }
      }
      return new ConcatPipe<>(outputPipes);
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    FileUtils.deleteFiles(tmpSubFolder);
  }

}
