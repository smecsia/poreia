package org.poreia.core.api.queue

import java.util.UUID.randomUUID

interface Queue<M> : Terminable {
    @Throws(InterruptedException::class)
    fun add(message: M)

    fun buildConsumer(name: String = randomUUID().toString()): QueueConsumer<M>

    fun isEmpty(): Boolean

    fun count(): Int
}
