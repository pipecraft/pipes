package org.pipecraft.pipes.sync.inter.reduct;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import java.io.IOException;

/**
 * Scans the input pipe and performs a reduction operation on a sequence of items with some discriminating property. 
 * The discrimination logic and reduction logic is provided by the caller.
 *
 * This pipe is the cheaper alternative to {@link HashReductorPipe}, and it's usually helpful only when the input item
 * are already grouped in families (such as they are after sorting).
 *
 * @param <I> The data type of the input items
 * @param <O> The data type of output items
 *
 * @author Eyal Schneider
 */
public class SequenceReductorPipe<I, O> implements Pipe<O> {
  private final Pipe<I> input;
  private final ReductorConfig<I, Object, Object, O> reductorConfig;
  private O next;

  /**
   * Constructor
   * 
   * @param input The input pipe to wrap
   * @param reductorConfig The reduction configuration
   */
  @SuppressWarnings("unchecked")
  public SequenceReductorPipe(Pipe<I> input, ReductorConfig<I, ?, ?, O> reductorConfig) {
    this.input = input;
    this.reductorConfig = (ReductorConfig<I, Object, Object, O>) reductorConfig;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public O next() throws PipeException, InterruptedException {
    O res = next;
    prepareNext();
    return res;
  }

  @Override
  public O peek() {
    return next;
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    prepareNext();
  }
  
  private void prepareNext() throws PipeException, InterruptedException {
    I peeked = input.peek();
    if (peeked == null) {
      next = null;
      return;
    }

    FailableFunction<I, Object, PipeException> discriminator = reductorConfig.getDiscriminator();
    Object familyId = discriminator.apply(peeked);
    Object aggregator = reductorConfig.getAggregatorCreator().apply(familyId);
    I v;
    while ((v = input.peek()) != null && discriminator.apply(v).equals(familyId)) {
      input.next();
      reductorConfig.getAggregationLogic().accept(aggregator, v);
    }
    
    next = reductorConfig.getPostProcessor().apply(aggregator);
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
