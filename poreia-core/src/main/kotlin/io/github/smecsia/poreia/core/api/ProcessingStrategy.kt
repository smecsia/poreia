package io.github.smecsia.poreia.core.api

interface ProcessingStrategy<M> {
    fun process(message: M): M
}

typealias ProcessingFnc<M> = (M) -> M

typealias ProcessAndStopFnc<M> = (M) -> Unit
