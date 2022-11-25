package io.github.smecsia.poreia.core.api.queue

import java.util.UUID.randomUUID

/**
 * Queue interface represents the queue allowing to add messages and build consumers
 */
interface Queue<M> : Terminable {
    /**
     * Append message to the queue
     */
    @Throws(InterruptedException::class)
    fun add(message: M)

    /**
     * Builds the consumer for the queue
     * @return [QueueConsumer] for the specific queue
     */
    fun buildConsumer(name: String = randomUUID().toString()): QueueConsumer<M>

    /**
     * Validates whether the queue is empty (if possible)
     * NOTE: this method may not have implementation in specific providers
     */
    fun isEmpty(): Boolean = throw NotImplementedError()

    /**
     * Returns the count of messages in the queue (if possible)
     * NOTE: this method may not have implementation in specific providers
     */
    fun count(): Int = throw NotImplementedError()
}
