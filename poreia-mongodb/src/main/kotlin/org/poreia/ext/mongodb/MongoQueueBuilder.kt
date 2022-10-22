package org.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.queue.QueueBuilder
import org.poreia.ext.mongodb.MongoQueue.Companion.QueueOpts

class MongoQueueBuilder<M : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: ToBsonSerializer<M> = DefaultToBsonSerializer(),
    private val collection: (target: String) -> String = { "queue_$it" },
    private val queueOpts: QueueOpts = QueueOpts()
) : QueueBuilder<M, Queue<M>> {

    override fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): Queue<M> {
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
