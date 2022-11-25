package io.github.smecsia.poreia.core.api.processing

/**
 * Represents the strategy for calculating the aggregation keys for [Aggregator] instances based on the incoming message
 */
interface AggregationKey<M> {
    fun calculate(message: M): String
}

typealias AggregationKeyFnc<M> = (M) -> String
