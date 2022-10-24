package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.api.ProcessingFnc
import io.github.smecsia.poreia.core.api.ProcessingStrategy

fun <M, S> aggregate(strategy: AggregationFnc<S, M>): AggregationStrategy<M, S> = object :
    AggregationStrategy<M, S> {
    override fun process(state: S, message: M): M {
        return strategy(state, message)
    }
}

fun <M> process(strategy: ProcessingFnc<M>): ProcessingStrategy<M> = object :
    ProcessingStrategy<M> {
    override fun process(message: M): M {
        return strategy(message)
    }
}

fun <M> key(func: AggregationKeyFnc<M>): AggregationKey<M> = object :
    AggregationKey<M> {
    override fun calculate(message: M): String {
        return func(message)
    }
}

fun <M> filter(filter: FilterFnc<M>): Filter<M> = object :
    Filter<M> {
    override fun filter(message: M): Boolean {
        return filter(message)
    }
}
