package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts

/**
 * Builder for [Aggregator] instances
 */
interface AggregatorBuilder<M, S> {
    fun build(
        pipeline: Pipeline<M, S>,
        name: String,
        strategy: AggregationStrategy<M, S>,
        key: AggregationKey<M>,
        repository: Repository<S>,
        filter: Filter<M>? = null,
        opts: Opts = Opts(),
    ): Aggregator<M, S>
}
