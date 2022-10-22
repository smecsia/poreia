package org.poreia.ext.mongodb

import org.poreia.core.api.Opts
import org.poreia.core.api.queue.QueueConsumer
import org.poreia.core.api.queue.QueueConsumerCallback
import org.poreia.core.api.queue.ackOnError
import org.poreia.ext.mongodb.MongoQueue.Companion.QueueOpts
import org.poreia.ext.mongodb.core.MongoQueueCore
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep

class MongoQueueConsumer<M>(
    private val queue: MongoQueueCore,
    private val serializer: ToBsonSerializer<M>,
    override val queueName: String,
    private val queueOpts: QueueOpts,
    private val opts: Opts = Opts(),
) : QueueConsumer<M> {

    override fun consume(callback: QueueConsumerCallback<M>) {
        val doc = try {
            queue.get(
                pollIntervalMs = queueOpts.pollIntervalMs,
                timeoutMs = Long.MAX_VALUE // block forever
            )
        } catch (e: Throwable) {
            LOG.warn("Failed to consume message from Mongo queue ${queue.collection.namespace.collectionName}: ${e.message}")
            sleep(1000)
            null
        }
        doc?.let { document ->
            LOG.debug("Got document {} from queue ${queue.collection.namespace.collectionName}", document.toJson())
            LOG.debug(
                "Trying to deserialize document {} from queue ${queue.collection.namespace.collectionName}",
                document.toJson()
            )
            val res = serializer.deserialize(document)
            LOG.debug("Deserialized to {} from queue ${queue.collection.namespace.collectionName}", res)
            callback.ackOnError(res, ackOnError = opts.ackOnError) {
                queue.ack(document)
            }
        } ?: throw MongoQueueException("null returned from mongo queue (and is not expected)")
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoQueueConsumer::class.java)
    }
}
