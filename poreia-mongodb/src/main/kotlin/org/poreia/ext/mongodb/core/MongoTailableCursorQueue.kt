package org.poreia.ext.mongodb.core

import com.mongodb.CursorType.TailableAwait
import com.mongodb.MongoException
import com.mongodb.client.MongoClient
import com.mongodb.client.model.CreateCollectionOptions
import org.bson.codecs.configuration.CodecRegistry
import org.poreia.ext.mongodb.DefaultToBsonSerializer
import org.poreia.ext.mongodb.ToBsonSerializer
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.function.Consumer

class MongoTailableCursorQueue<T : Any> @JvmOverloads constructor(
    mongo: MongoClient,
    dbName: String,
    collection: String,
    serializer: ToBsonSerializer<T> = DefaultToBsonSerializer(),
    private val opts: QueueOpts = QueueOpts(),
    codecRegistry: CodecRegistry? = null
) : AbstractMongoRepo<T>(mongo, dbName, collection, serializer, codecRegistry),
    Queue<T> {

    @Volatile
    var stopped = false
    override fun drop() {
        collection().drop()
    }

    override fun init(): MongoTailableCursorQueue<T> {
        if (db().listCollectionNames().none { s: String -> s == collection }) {
            db().createCollection(
                collection,
                CreateCollectionOptions().capped(true)
                    .maxDocuments(opts.maxSize)
                    .sizeInBytes(opts.maxDocSize * opts.maxSize)
            )
        }
        return this
    }

    override fun stop() {
        stopped = true
    }

    override fun poll(consumer: Consumer<T>) {
        if (stopped) {
            LOGGER.warn("Could not stopped queue {}.{}", dbName, collection)
        }
        while (!stopped) {
            try {
                collection().find()
                    .cursorType(TailableAwait)
                    .noCursorTimeout(true)
                    .batchSize(opts.batchSize).forEach { doc ->
                        consumer.accept(documentToValue(doc)!!)
                    }
                LOGGER.debug(
                    "Tailable cursor returned no value without await for {}.{}",
                    dbName,
                    collection
                )
                sleep(opts.sleepBetweenFailuresMs)
            } catch (e: MongoException) {
                LOGGER.debug(
                    "Failed to iterate on queue cursor for {}.{}",
                    dbName,
                    collection,
                    e
                )
                sleep(opts.sleepBetweenFailuresMs)
            }
        }
    }

    override fun add(obj: T) {
        collection().insertOne(valueToDocument(obj))
    }

    override fun size(): Long {
        return collection().countDocuments()
    }

    data class QueueOpts(
        val maxSize: Long = DEFAULT_MAX_SIZE,
        val maxDocSize: Long = ASSUMED_MAX_DOC_SIZE,
        val batchSize: Int = BATCH_SIZE,
        val sleepBetweenFailuresMs: Long = SLEEP_BETWEEN_FAILURES_MS,
        val minPollIntervalMs: Long = MIN_POLL_INTERVAL_MS
    )

    companion object {
        const val DEFAULT_MAX_SIZE = 1000L
        const val ASSUMED_MAX_DOC_SIZE = 1024L * 1024L // 1mb
        const val BATCH_SIZE = 100
        const val SLEEP_BETWEEN_FAILURES_MS = 500L
        const val MIN_POLL_INTERVAL_MS = 100L
        private val LOGGER = LoggerFactory.getLogger(MongoTailableCursorQueue::class.java)
    }
}
