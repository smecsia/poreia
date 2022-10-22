package org.poreia.core.api.processing

interface Filter<M> {
    fun filter(message: M): Boolean
}

typealias FilterFnc<M> = (M) -> Boolean
