package io.github.smecsia.poreia.core.api.queue

typealias QueueConsumerCallback<M> = (M) -> Unit

/**
 * Queue consumer allowing to consume messages from the queue
 */
interface QueueConsumer<M> : Terminable {
    /**
     * Blocks until new message appears in queue
     * @param callback message processor callback
     * @return new message from the queue
     */
    @Throws(InterruptedException::class)
    fun consume(callback: QueueConsumerCallback<M>)

    /**
     * Name of the queue the consumer is built for
     */
    val queueName: String
}

fun <M> QueueConsumerCallback<M>.ackOnError(
    msg: M,
    ackOnError: Boolean = false,
    ackCallback: () -> Unit,
) {
    try {
        this(msg)
    } catch (e: Throwable) {
        if (ackOnError) ackCallback()
        throw e
    }
    ackCallback()
}
