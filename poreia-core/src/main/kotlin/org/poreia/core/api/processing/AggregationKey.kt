package org.poreia.core.api.processing

interface AggregationKey<M> {
    fun calculate(message: M): String
}

typealias AggregationKeyFnc<M> = (M) -> String
