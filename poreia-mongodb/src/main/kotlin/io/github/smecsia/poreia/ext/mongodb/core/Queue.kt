package io.github.smecsia.poreia.ext.mongodb.core

import java.util.function.Consumer

interface Queue<T> {
    /**
     * Initialize the queue
     */
    fun init(): Queue<T> { return this }

    /**
     * Drop the queue
     */
    fun drop() {}

    /**
     * Stop the processing of the messages
     * This method makes sense only when there is an
     * active poll process
     */
    fun stop()

    /**
     * Polls the queue and gives the control to consumer upom each incoming item within
     */
    fun poll(consumer: Consumer<T>)

    /**
     * Appends the new item to the queue
     */
    fun add(obj: T)

    /**
     * Returns the size of the queue (number of documents currently enqueued)
     */
    fun size(): Long
}
