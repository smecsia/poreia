package io.github.smecsia.poreia.core.api.processing

/**
 * Base interface allowing to filter incoming messages
 */
interface Filter<M> {
    fun filter(message: M): Boolean
}

typealias FilterFnc<M> = (M) -> Boolean
