package io.github.smecsia.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.Projections.include
import io.github.smecsia.poreia.core.api.processing.Locker
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.error.InvalidLockOwnerException
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import io.github.smecsia.poreia.core.util.ThreadUtil.threadId
import io.github.smecsia.poreia.ext.mongodb.core.AbstractMongoRepo
import io.github.smecsia.poreia.ext.mongodb.core.MongoLockingSupport
import org.bson.Document
import org.slf4j.LoggerFactory

class MongoKVRepository<T : Any>(
    mongo: MongoClient,
    dbName: String,
    collection: String,
    serializer: ToBsonSerializer<T> = DefaultToBsonSerializer(),
    private val locker: Locker = MongoLockingSupport(mongo, dbName, "${collection}_lock"),
    override val stateInitializer: StateInitializer<T>? = null,
) : AbstractMongoRepo<T>(mongo, dbName, collection, serializer),
    Repository<T>,
    Locker by locker {

    @Throws(LockWaitTimeoutException::class)
    override fun lockAndGet(key: String): T? {
        LOGGER.trace("Trying lock and get key {} by thread {}", key, threadId())
        lock(key)
        return get(key)
    }

    override fun setAndUnlock(key: String, value: T): T {
        LOGGER.trace("Putting new value and unlocking key {} by thread {}", key, threadId())
        ensureLockOwner(key)
        set(key, value)
        unlock(key)
        return value
    }

    @Throws(InvalidLockOwnerException::class)
    override fun deleteAndUnlock(key: String) {
        LOGGER.trace("Removing value and unlocking key {} by thread {}", key, threadId())
        ensureLockOwner(key)
        delete(key)
        unlock(key)
    }

    override fun forceDeleteAndUnlock(key: String) {
        LOGGER.trace("Forcing removing value and unlocking key {} by thread {}", key, threadId())
        delete(key)
        forceUnlock(key)
    }

    fun delete(key: String) {
        LOGGER.trace(
            "Removing value without unlocking key {} by thread {}",
            key,
            threadId(),
        )
        collection().deleteOne(byId(key))
    }

    override operator fun set(key: String, value: T): T {
        collection().findOneAndReplace(byId(key), valueToDocument(value), FindOneAndReplaceOptions().upsert(true))
        return value
    }

    override operator fun get(key: String): T? {
        return documentToValue(collection().find(byId(key)).first())
    }

    override fun keys(): Collection<String> {
        return collectionDoc().find()
            .projection(include("_id"))
            .map { it["_id"] as String }.toSet()
    }

    override fun values(): Map<String, T> {
        val collection = db().getCollection(collection, Document::class.java)
        return collection.find().map {
            it["_id"] as String to documentToValue(it)!!
        }.toMap()
    }

    private fun ensureLockOwner(key: String) {
        if (!isLockedByMe(key)) {
            throw InvalidLockOwnerException("Key '$key' is not locked by threadId '${threadId()}'!")
        }
    }

    private fun byId(key: String?): Document {
        return Document("_id", key)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(MongoKVRepository::class.java)
    }

    override fun clear() {
        collection().drop()
    }
}
