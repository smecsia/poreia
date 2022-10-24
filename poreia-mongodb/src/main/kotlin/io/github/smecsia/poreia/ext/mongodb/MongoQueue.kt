package io.github.smecsia.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueConsumer
import io.github.smecsia.poreia.ext.mongodb.core.MongoQueueCore

class MongoQueue<M : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val collectionName: String,
    private val queueName: String,
    private val serializer: ToBsonSerializer<M>,
    private val queueOpts: QueueOpts,
    private val opts: Opts = Opts(),
) : Queue<M> {
    private val queue = MongoQueueCore(
        mongo.getDatabase(dbName).getCollection(collectionName),
    )

    override fun add(message: M) {
        queue.send(serializer.serialize(message))
    }

    override fun buildConsumer(name: String): QueueConsumer<M> {
        return MongoQueueConsumer(
            queue = queue,
            serializer = serializer,
            queueName = queueName,
            queueOpts = queueOpts,
            opts = opts,
        )
    }

    companion object {
        data class QueueOpts(
            val pollIntervalMs: Long = 200,
        )
    }

    override fun isEmpty(): Boolean = count() == 0

    override fun count(): Int = queue.count().toInt()
}
