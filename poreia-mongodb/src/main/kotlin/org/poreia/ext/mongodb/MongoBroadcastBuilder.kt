package org.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import org.bson.codecs.configuration.CodecRegistry
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.BroadcastBuilder
import org.poreia.core.api.queue.Broadcaster
import org.poreia.ext.mongodb.core.MongoTailableCursorQueue
import org.poreia.ext.mongodb.core.MongoTailableCursorQueue.QueueOpts

class MongoBroadcastBuilder<M : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: ToBsonSerializer<M> = DefaultToBsonSerializer(),
    private val codecRegistry: CodecRegistry? = null,
    val collection: (target: String) -> String = { "bc_to_$it" },
    private val queueOpts: QueueOpts = QueueOpts()
) : BroadcastBuilder<M> {

    override fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        val queue = MongoTailableCursorQueue(
            mongo = mongo,
            dbName = dbName,
            collection = collection(target),
            serializer = serializer,
            opts = queueOpts,
            codecRegistry = codecRegistry
        ).init()
        return MongoBroadcaster(
            target = target,
            pipeline = pipeline,
            queue = queue,
            opts = opts
        )
    }
}
