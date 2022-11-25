package io.github.smecsia.poreia.core.api.processing

/**
 * Base interface for aggregation of the messages
 */
interface Aggregator<M, S> : Processor<M> {
    val repository: Repository<S>
}
