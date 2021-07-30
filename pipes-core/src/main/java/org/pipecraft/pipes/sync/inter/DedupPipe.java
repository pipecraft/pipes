package org.pipecraft.pipes.sync.inter;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.sync.inter.reduct.HashReductorPipe;
import org.pipecraft.pipes.sync.inter.reduct.ReductorConfig;
import java.io.File;
import java.util.function.Function;

/**
 * Uses item equality (equals() method) in input pipe items for performing a dedup operation.
 * Only one arbitrary instance per equivalence set is produced in the output.
 *
 * The implementation relies on equals() and on consistency of equals() with hashcode().
 *
 * This pipe makes use of temporary disk space.
 * The configuration of partitionCount is critical for bounding memory usage. The more partitions are used, the less memory is required.
 * It's recommended to set this number to {estimated total input data volume} / {max memory allowed for this pipe to use}.
 *
 * @param <T> The data type of items in the input pipe
 *
 * @author Eyal Schneider
 */
public class DedupPipe<T> extends HashReductorPipe<T, T> {

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
   */
  public DedupPipe(Pipe<T> input, CodecFactory<T> inputCodec, int partitionCount, File tmpFolder) {
    super(input, inputCodec, partitionCount, tmpFolder, ReductorConfig.<T, T, T, T>builder()
        .discriminator(FailableFunction.identity())
        .aggregatorCreator(Function.identity())
        .aggregationLogic((g, v) -> {})
        .postProcessor(Function.identity()).build());
  }
}
