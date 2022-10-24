package io.github.smecsia.poreia.ext.mongodb.core

import com.mongodb.MongoCommandException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.UpdateOptions
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.time.Instant
import java.time.Instant.now
import java.util.Date
import java.util.UUID.randomUUID

class MongoQueueCore(val collection: MongoCollection<Document>) {

    /**
     * Ensure index for get() method
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     * @param afterSort  fields in get() call that should be after the sort fields in the index. Should not be null
     */
    @JvmOverloads
    fun ensureGetIndex(beforeSort: Document = Document(), afterSort: Document = Document()) {
        // using general rule: equality, sort, range or more equality tests in that order for index
        val completeIndex = Document("running", 1)
        ensureOrder(beforeSort, completeIndex)
        completeIndex.append("priority", 1).append("created", 1)
        ensureOrder(afterSort, completeIndex)
        completeIndex.append("earliestGet", 1)
        ensureIndex(completeIndex) // main query in Get()
        ensureIndex(Document("running", 1).append("resetTimestamp", 1)) // for the stuck messages query in Get()
    }

    /**
     * Ensure index for count() method
     *
     * @param index          fields in count() call. Should not be null
     * @param includeRunning whether running was given to count() or not
     */
    fun ensureCountIndex(index: Document, includeRunning: Boolean) {
        val completeIndex = Document()
        if (includeRunning) {
            completeIndex.append("running", 1)
        }
        ensureOrder(index, completeIndex)
        ensureIndex(completeIndex)
    }

    /**
     * Get a non running message from queue
     *
     * @param query         query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null.
     * @param resetDurationMs duration in ms before this message is considered abandoned and will be given with another call to get()
     * @param timeoutMs  duration in milliseconds to keep polling before returning null
     * @param pollIntervalMs  duration in milliseconds between poll attempts
     * @return message or null
     */
    @JvmOverloads
    operator fun get(
        query: Document = Document(),
        resetDurationMs: Int = Int.MAX_VALUE,
        timeoutMs: Long = Long.MAX_VALUE,
        pollIntervalMs: Long = 200,
        earliestGet: Instant = now(),
    ): Document? {
        // reset stuck messages
        collection.updateMany(
            Document("running", true).append("resetTimestamp", Document("\$lte", earliestGet.toEpochMilli())),
            Document("\$set", Document("running", false)),
            UpdateOptions().upsert(false),
        )
        val builtQuery = Document("running", false)
        for ((key, value) in query) {
            builtQuery.append("payload.$key", value)
        }
        builtQuery.append("earliestGet", Document("\$lte", earliestGet.toEpochMilli()))
        val resetTimestamp = earliestGet.plusMillis(resetDurationMs.toLong())
        val sort = Document("priority", 1).append("created", 1)
        val update =
            Document("\$set", Document("running", true).append("resetTimestamp", resetTimestamp.toEpochMilli()))
        val fields = Document("payload", 1)
        val waitUntil = earliestGet.plusMillis(timeoutMs)
        while (true) {
            val opts = FindOneAndUpdateOptions().sort(sort).upsert(false)
                .returnDocument(ReturnDocument.AFTER)
                .projection(fields)
            LOG.trace(
                "EXECUTING: db.{}.findOneAndUpdate({},{})",
                collection.namespace.collectionName,
                builtQuery.toJson(),
                update.toJson(),
            )
            val message = collection.findOneAndUpdate(builtQuery, update, opts)
            LOG.trace(
                "RESULT: db.{}.findOneAndUpdate({},{}):\n {}",
                collection.namespace.collectionName,
                builtQuery.toJson(),
                update.toJson(),
                message,
            )
            if (message != null) {
                try {
                    val id = message.getObjectId("_id")
                    return (message["payload"] as Document).append("id", id)
                } catch (e: Throwable) {
                    LOG.error("Failed to process message {}", message, e)
                }
            }
            builtQuery.append("earliestGet", Document("\$lte", now().toEpochMilli()))
            if (now().isAfter(waitUntil)) {
                return null
            }
            try {
                if (pollIntervalMs <= 0L) {
                    continue
                }
                sleep(pollIntervalMs)
            } catch (ex: InterruptedException) {
                throw RuntimeException(ex)
            }
        }
    }

    /**
     * Count in queue
     *
     * @param query   query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null
     * @param running count running messages or not running
     * @return count
     */
    fun count(running: Boolean? = null, query: Document? = null): Long {
        val actualQuery = Document()
        running?.let { actualQuery.append("running", it) }
        query?.let {
            it.forEach { kv ->
                actualQuery.append("payload.${kv.key}", kv.value)
            }
        }
        return collection.countDocuments(actualQuery)
    }

    /**
     * Acknowledge a message was processed and remove from queue
     *
     * @param message message received from get(). Should not be null.
     */
    fun ack(message: Document) {
        val id = message["id"]
        require(id is ObjectId) { "id must be an ObjectId" }
        collection.deleteOne(Document("_id", id))
    }

    /**
     * Ack message and send payload to queue, atomically
     *
     * @param message     message to ack received from get(). Should not be null
     * @param payload     payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     * @param priority    priority for order out of get(). 0 is higher priority than 1. Should not be NaN
     */
    @JvmOverloads
    fun ackSend(message: Document, payload: Document, earliestGet: Instant = now(), priority: Double = 0.0) {
        require(!priority.isNaN()) { "priority was NaN" }
        val id = message["id"]
        require(id is ObjectId) { "id must be an ObjectId" }
        val newMessage = Document(
            "\$set",
            Document("payload", payload)
                .append("running", false)
                .append("resetTimestamp", Int.MAX_VALUE.toLong())
                .append("earliestGet", earliestGet.toEpochMilli())
                .append("priority", priority)
                .append("created", Date(now().toEpochMilli())),
        )

        // using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
        // collection.update(new Document("_id", id), newMessage, true, false);
        collection.updateOne(Filters.eq("_id", id), newMessage, UpdateOptions().upsert(true))
    }

    /**
     * Requeue message. Same as ackSend() with the same message.
     *
     * @param message     message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     * @param priority    priority for order out of get(). 0 is higher priority than 1. Should not be NaN
     */
    @JvmOverloads
    fun requeue(message: Document, earliestGet: Instant = now(), priority: Double = 0.0) {
        require(!priority.isNaN()) { "priority was NaN" }
        val id = message["id"]
        require(id is ObjectId) { "id must be an ObjectId" }
        val forRequeue = Document(message)
        forRequeue.remove("id")
        ackSend(message, forRequeue, earliestGet, priority)
    }

    /**
     * Send message to queue
     *
     * @param payload     payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return message. Should not be null
     * @param priority    priority for order out of Get(). 0 is higher priority than 1. Should not be NaN
     */
    @JvmOverloads
    fun send(payload: Document, earliestGet: Instant = now(), priority: Double = 0.0) {
        require(!priority.isNaN()) { "priority was NaN" }
        val message = Document("payload", payload)
            .append("running", false)
            .append("resetTimestamp", Int.MAX_VALUE.toLong())
            .append("earliestGet", earliestGet.toEpochMilli())
            .append("priority", priority)
            .append("created", Date(now().toEpochMilli()))
        collection.insertOne(message)
    }

    private fun ensureOrder(documents: Document, completeIndex: Document) {
        for ((key, value) in documents) {
            require(!(value != 1 && value != -1)) { "field values must be either 1 or -1" }
            completeIndex.append("payload.$key", value)
        }
    }

    private fun ensureIndex(index: Document) {
        for (i in 0..4) {
            var name = randomUUID().toString()
            while (name.isNotEmpty()) {
                // creating an index with the same name and different spec does nothing.
                // creating an index with different name and same spec does nothing.
                // so we use any generated name, and then find the right spec after we have called, and just go with that name.
                for (existingIndex in collection.listIndexes()) {
                    if (existingIndex["key"] == index) {
                        return
                    }
                }
                // create index if not found
                val iOpts = IndexOptions().background(true).name(name)
                try {
                    collection.createIndex(index, iOpts)
                } catch (e: MongoCommandException) {
                    when (e.errorCodeName) {
                        "IndexOptionsConflict" -> {
                            // ignore index existence
                        }
                        else -> throw e
                    }
                }
                name = name.substring(0, name.length - 1)
            }
        }
        throw RuntimeException("couldn't create index after 5 attempts")
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoQueueCore::class.java)
    }
}
