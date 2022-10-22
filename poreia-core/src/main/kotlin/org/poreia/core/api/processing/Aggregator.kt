package org.poreia.core.api.processing

interface Aggregator<M, S> : Processor<M> {
    val repository: Repository<S>
}
