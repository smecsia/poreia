package io.github.smecsia.poreia.core.api

/**
 * Defines how to process incoming messages
 */
interface ProcessingStrategy<M> {
    fun process(message: M): M
}

typealias ProcessingFnc<M> = (M) -> M

typealias ProcessAndStopFnc<M> = (M) -> Unit
