package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.sync.inter.reduct.HashReductorPipe;
import org.pipecraft.pipes.sync.inter.reduct.ReductorConfig;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Splits the input items into different families, and emits items from the same family sequentially.
 * The order between families is arbitrary, but the ordering of items inside each family as preserved like in the input.
 *
 * This pipe makes use of temporary disk space.
 * The configuration of partitionCount is critical for bounding memory usage. The more partitions are used, the less memory is required.
 * It's recommended to set this number to {estimated total input data volume} / {max memory allowed for this pipe to use}.
 *
 * @param <T> The data type of items in the input pipe
 *
 * @author Eyal Schneider
 */
public class GrouperPipe<T> extends CompoundPipe<T> {
  private final Pipe<T> input;
  private final FailableFunction<T, Object, PipeException> discriminator;
  private final CodecFactory<T> inputCodec;
  private final int partitionCount;
  private final File tmpFolder;

  /**
   * Constructor
   *
   * @param input The input pipe to wrap
   * @param discriminator The function that identifies which "family" an item belongs to.
   * Important: The value returned by the discriminator should have an equals() implementation which is consistent with the family partitioning and with the hashcode() method implementation.
   * @param inputCodec A codec allowing writing/reading input records
   * @param partitionCount The number of partitions to split input into.
   * Assuming a good hash function on item keys, and assuming that the families defined by the discriminator are even in size, the caller can assume the partitions are more-less balanced in size.
   * This number determines the amount of memory to be used, so it should be defined with caution. The more partitions are used, the less total memory is
   * required. However, note that for each partition the class maintains an open file on disk.
   * @param tmpFolder The folder where to store temporary data
   */
  @SuppressWarnings("unchecked")
  public GrouperPipe(Pipe<T> input, FailableFunction<T, ?, PipeException> discriminator, CodecFactory<T> inputCodec, int partitionCount, File tmpFolder) {
    this.input = input;
    this.discriminator = (FailableFunction<T, Object, PipeException>) discriminator;
    this.inputCodec = inputCodec;
    this.partitionCount = partitionCount;
    this.tmpFolder = tmpFolder;
  }

  @Override
  protected Pipe<T> createPipeline() throws PipeException, InterruptedException {
    Pipe<List<T>> reductorP = new HashReductorPipe<>(input, inputCodec, partitionCount, tmpFolder,
        ReductorConfig.<T, Object, List<T>, List<T>>builder()
            .discriminator(discriminator)
            .aggregatorCreator(f -> new ArrayList<>())
            .aggregationLogic(List::add)
            .postProcessor(Function.identity())
            .build());
    return new FlexibleMapPipe<>(reductorP, CollectionReaderPipe::new);
  }
}
