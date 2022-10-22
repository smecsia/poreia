package org.poreia.core.api.processing

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts

interface AggregatorBuilder<M, S> {
    fun build(
        pipeline: Pipeline<M, S>,
        name: String,
        strategy: AggregationStrategy<M, S>,
        key: AggregationKey<M>,
        repository: Repository<S>,
        filter: Filter<M>? = null,
        opts: Opts = Opts()
    ): Aggregator<M, S>
}
