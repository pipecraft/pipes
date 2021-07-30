package org.pipecraft.pipes.sync.inter.reduct;

import org.pipecraft.infra.concurrent.FailableBiConsumer;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import java.util.function.Function;

/**
 * A configuration for a reduction operation.
 * Reduction consists of identifying disjoint "families" of items in the input and aggregating
 * each family separately into some summary output item.
 *
 * Uses builder design pattern.
 *
 * @param <I> The data type of the input items
 * @param <F> The data type of the family identifier
 * @param <G> The intermediate form items get aggregated to. Must be mutable.
 * @param <O> The data type of output items
 *
 * @author Eyal Schneider
 */
public class ReductorConfig <I, F, G, O> {
  private final FailableFunction<I, F, PipeException> discriminator;
  private final Function<F, G> aggregatorCreator;
  private final FailableBiConsumer<G, I, PipeException> aggregationLogic;
  private final Function<G, O> postProcessor;

  /**
   * Private constructor
   *
   * @param discriminator The function that identifies which "family" an item belongs to. Items of the same family are being aggregated into the same aggregator object.
   * Important: The value returned by the discriminator should have an equals() implementation which is consistent with the family partitioning and with the hashcode() method implementation.
   * @param aggregatorCreator A generator of a blank new aggregator object of type G. The creator is given the family id in case that it's needed for initialization purposes.
   * @param aggregationLogic Accepts the current aggregator state (of the type denoted by G) and aggregates a given item of type I into it
   * @param postProcessor A converter from G to O, to be applied once the aggregation terminates and we are ready to produce an output item
   */
  private ReductorConfig(FailableFunction<I, F, PipeException> discriminator, Function<F, G> aggregatorCreator, FailableBiConsumer<G, I, PipeException> aggregationLogic, Function<G, O> postProcessor) {
    this.discriminator = discriminator;
    this.aggregatorCreator = aggregatorCreator;
    this.aggregationLogic = aggregationLogic;
    this.postProcessor = postProcessor;
  }

  /**
   * @return A new builder to use for defining the reductor settings
   */
  public static <I, F, G, O> Builder<I, F, G, O> builder() {
    return new Builder<>();
  }

  /**
   * @return The function that identifies which "family" an item belongs to. Items of the same family are being aggregated into the same aggregator object.
   */
  public FailableFunction<I, F, PipeException> getDiscriminator() {
    return discriminator;
  }

  /**
   * @return The generator of a blank new aggregator object of type G. The creator is given the family id in case that it's needed for initialization purposes.
   */
  public Function<F, G> getAggregatorCreator() {
    return aggregatorCreator;
  }

  /**
   * @return the logic that accepts the current aggregator state (of the type denoted by G) and aggregates a given item of type I into it
   */
  public FailableBiConsumer<G, I, PipeException> getAggregationLogic() {
    return aggregationLogic;
  }

  /**
   * @return The converter from G to O, to be applied once the aggregation terminates and we are ready to produce an output item
   */
  public Function<G, O> getPostProcessor() {
    return postProcessor;
  }

  public static class Builder<I, F, G, O> {
    private FailableFunction<I, F, PipeException> discriminator;
    private Function<F, G> aggregatorCreator;
    private FailableBiConsumer<G, I, PipeException> aggregationLogic;
    private Function<G, O> postProcessor;

    /**
     * Private constructor
     */
    private Builder() {
    }

    /**
     * @param discriminator The function that identifies which "family" an item belongs to. Items of the same family are being aggregated into the same aggregator object.
     * @return This builder
     */
    public Builder<I, F, G, O> discriminator(FailableFunction<I, F, PipeException> discriminator) {
      this.discriminator = discriminator;
      return this;
    }

    /**
     * @param aggregatorCreator A generator of a blank new aggregator object of type G. The creator is given the family id in case that it's needed for initialization purposes.
     * @return This builder
     */
    public Builder<I, F, G, O> aggregatorCreator(Function<F, G> aggregatorCreator) {
      this.aggregatorCreator = aggregatorCreator;
      return this;
    }

    /**
     * @param aggregationLogic Accepts the current aggregator state (of the type denoted by G) and aggregates a given item of type I into it
     * @return This builder
     */
    public Builder<I, F, G, O> aggregationLogic(FailableBiConsumer<G, I, PipeException> aggregationLogic) {
      this.aggregationLogic = aggregationLogic;
      return this;
    }

    /**
     * @param postProcessor A converter from G to O, to be applied once the aggregation terminates and we are ready to produce an output item
     * @return This builder
     */
    public Builder<I, F, G, O> postProcessor(Function<G, O> postProcessor) {
      this.postProcessor = postProcessor;
      return this;
    }

    /**
     * @return The function that identifies which "family" an item belongs to. Items of the same family are being aggregated into the same aggregator object.
     */
    public FailableFunction<I, F, PipeException> getDiscriminator() {
      return discriminator;
    }

    /**
     * @return The generator of a blank new aggregator object of type G. The creator is given the family id in case that it's needed for initialization purposes.
     */
    public Function<F, G> getAggregatorCreator() {
      return aggregatorCreator;
    }

    /**
     * @return the logic that accepts the current aggregator state (of the type denoted by G) and aggregates a given item of type I into it
     */
    public FailableBiConsumer<G, I, PipeException> getAggregationLogic() {
      return aggregationLogic;
    }

    /**
     * @return The converter from G to O, to be applied once the aggregation terminates and we are ready to produce an output item
     */
    public Function<G, O> getPostProcessor() {
      return postProcessor;
    }

    /**
     * @return The new immutable configuration object
     */
    public ReductorConfig<I, F, G, O> build() {
      return new ReductorConfig<>(discriminator, aggregatorCreator, aggregationLogic, postProcessor);
    }
  }
}
