package io.github.smecsia.poreia.core.api.processing

interface Aggregator<M, S> : Processor<M> {
    val repository: Repository<S>
}
