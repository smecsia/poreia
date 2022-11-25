package io.github.smecsia.poreia.core.api.queue

/**
 * Unlike [Queue] represents the broadcasting message poster (message will be routed to all connected processors of the
 * specific name)
 */
interface Broadcaster<M> : Terminable {
    fun broadcast(message: M)
}
