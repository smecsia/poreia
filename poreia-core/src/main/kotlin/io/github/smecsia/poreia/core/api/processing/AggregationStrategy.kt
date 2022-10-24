package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.api.ProcessingStrategy

interface AggregationStrategy<M, S> : ProcessingStrategy<M> {
    fun process(state: S, message: M): M = message

    override fun process(message: M): M = message
}

typealias AggregationFnc<S, M> = (S, M) -> M
