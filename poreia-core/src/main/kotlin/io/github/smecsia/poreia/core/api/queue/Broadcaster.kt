package io.github.smecsia.poreia.core.api.queue

interface Broadcaster<M> : Terminable {
    fun broadcast(message: M)
}
