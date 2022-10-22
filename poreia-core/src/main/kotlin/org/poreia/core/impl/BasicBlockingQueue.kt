package org.poreia.core.impl

import org.poreia.core.api.queue.Queue
import org.poreia.core.api.queue.QueueConsumer
import org.poreia.core.api.queue.QueueConsumerCallback
import org.slf4j.LoggerFactory
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

class BasicBlockingQueue<M>(
    private val name: String,
    private val maxSize: Int = DEFAULT_MAX_SIZE,
    private val queue: BlockingQueue<M> = ArrayBlockingQueue(maxSize)
) : Queue<M> {
    @Throws(InterruptedException::class)
    override fun add(message: M) {
        queue.put(message)
    }

    override fun buildConsumer(name: String): QueueConsumer<M> {
        return object : QueueConsumer<M> {
            override fun consume(callback: QueueConsumerCallback<M>) {
                callback(queue.take())
            }

            override val queueName: String
                get() = name
        }
    }

    companion object {
        const val DEFAULT_MAX_SIZE = 100000
        private val LOGGER = LoggerFactory.getLogger(BasicBlockingQueue::class.java)
    }

    init {
        LOGGER.info("Initializing basic blocking queue $name with max size $maxSize...")
    }

    override fun isEmpty(): Boolean = queue.isEmpty()

    override fun count(): Int = queue.size
}
