package org.pipecraft.pipes.sync.inter.reduct;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.pipecraft.pipes.exceptions.ExcessiveResourcesUsagePipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * Scans the input pipe and groups sequential items based on some discriminating property. The group is then being reduced into a single item
 * using the caller's provided logic. 
 * Since each sequence is internally stored in a list, this class provides protection defined by groupSizeLimit and sizeLimitPolicy.
 * 
 * @param <S> The data type of items in the input pipe
 * @param <T> The data type of the output items
 *
 * @author Eyal Schneider
 */
public class ListReductorPipe<S, T> extends SequenceReductorPipe<S, T> {
  public enum GroupSizeLimitPolicy {TRUNCATE, FAIL}

  /**
   * Constructor
   * 
   * @param input The input pipe
   * @param discriminator The function that maps input pipe values to some discriminator value used for reduction
   * @param postProcessor A function that accepts a list of values to group (i.e. having the same discriminator value), and aggregates them into an output item.
   * @param groupSizeLimit The maximum size for a group. Used for avoiding excessive heap usage when collecting items for the reductor. Use 0 for no limit.
   * @param sizeLimitPolicy Use TRUNCATE for ignoring group items beyond the threshold, and FAIL for throwing an exception when this happens. 
   */
  @SuppressWarnings("unchecked")
  public ListReductorPipe(Pipe<S> input, FailableFunction<S, ?, PipeException> discriminator, Function<List<S>, T> postProcessor, int groupSizeLimit, GroupSizeLimitPolicy sizeLimitPolicy) {
    super(input,
        ReductorConfig.<S, Object, List<S>, T>builder()
        .discriminator((FailableFunction<S, Object, PipeException>) discriminator)
        .aggregatorCreator(v -> new ArrayList<>())
        .aggregationLogic((list, v) -> aggregate(list, v, groupSizeLimit, sizeLimitPolicy))
        .postProcessor(postProcessor).build());
  }

  /**
   * Constructor
   * 
   * Creates a sequence reductor pipe with no protective threshold on group sizes.
   * @param input The input pipe
   * @param discriminator The function that maps input pipe values to some discriminator value used for reduction
   * @param postProcessor A function that accepts a list of values to group (i.e. having the same discriminator value), and aggregates them into an output item.
   */
  public ListReductorPipe(Pipe<S> input, FailableFunction<S, ?, PipeException> discriminator, Function<List<S>, T> postProcessor) {
    this(input, discriminator, postProcessor, 0, null);
  }

  private static <V> void aggregate(List<V> list, V v, int groupSizeLimit, GroupSizeLimitPolicy sizeLimitPolicy) throws PipeException {
    if (groupSizeLimit > 0 && list.size() == groupSizeLimit) {
      switch (sizeLimitPolicy) {
        case TRUNCATE : return; 
        case FAIL : 
          throw new ExcessiveResourcesUsagePipeException("Sequence reductor encountered more than " + groupSizeLimit + " items in the same group.");
      }
    }
    list.add(v);
  }
}
