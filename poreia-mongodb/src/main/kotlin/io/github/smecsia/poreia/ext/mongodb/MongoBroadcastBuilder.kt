package io.github.smecsia.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.ext.mongodb.core.MongoTailableCursorQueue
import io.github.smecsia.poreia.ext.mongodb.core.MongoTailableCursorQueue.QueueOpts
import org.bson.codecs.configuration.CodecRegistry

class MongoBroadcastBuilder<M : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: io.github.smecsia.poreia.ext.mongodb.ToBsonSerializer<M> = io.github.smecsia.poreia.ext.mongodb.DefaultToBsonSerializer(),
    private val codecRegistry: CodecRegistry? = null,
    val collection: (target: String) -> String = { "bc_to_$it" },
    private val queueOpts: QueueOpts = QueueOpts(),
) : BroadcastBuilder<M> {

    override fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        val queue = MongoTailableCursorQueue(
            mongo = mongo,
            dbName = dbName,
            collection = collection(target),
            serializer = serializer,
            opts = queueOpts,
            codecRegistry = codecRegistry,
        ).init()
        return io.github.smecsia.poreia.ext.mongodb.MongoBroadcaster(
            target = target,
            pipeline = pipeline,
            queue = queue,
            opts = opts,
        )
    }
}
