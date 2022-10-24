package io.github.smecsia.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import io.github.smecsia.poreia.ext.mongodb.MongoQueue.Companion.QueueOpts

class MongoQueueBuilder<M : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: ToBsonSerializer<M> = DefaultToBsonSerializer(),
    private val collection: (target: String) -> String = { "queue_$it" },
    private val queueOpts: QueueOpts = QueueOpts(),
) : QueueBuilder<M, Queue<M>> {

    override fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Queue<M> {
        return MongoQueue(
            mongo = mongo,
            dbName = dbName,
            serializer = serializer,
            collectionName = collection(name),
            queueName = name,
            queueOpts = queueOpts,
            opts = opts,
        )
    }
}
