package org.poreia.ext.mongodb.core

import com.mongodb.MongoWriteException
import com.mongodb.client.MongoClient
import com.mongodb.client.model.UpdateOptions
import org.bson.Document
import org.poreia.core.error.InvalidLockOwnerException
import org.poreia.core.error.LockWaitTimeoutException
import org.poreia.ext.mongodb.core.LockingSupport.LockOpts
import org.poreia.core.util.ThreadUtil.threadId
import org.slf4j.LoggerFactory
import java.lang.System.currentTimeMillis
import java.lang.Thread.sleep
import java.util.Random

class MongoLockingSupport(
    mongo: MongoClient,
    dbName: String,
    collection: String,
    override val opts: LockOpts = LockOpts()
) : AbstractMongoRepo<Document>(mongo, dbName, collection), LockingSupport {

    @Throws(LockWaitTimeoutException::class)
    override fun tryLock(key: String, timeoutMs: Long) {
        val waitStartedTime = currentTimeMillis()
        LOGGER.trace("Trying to lock the key '{}' for threadId '{}'...", key, threadId())
        if (isLockedByMe(key)) {
            return
        }
        while (!tryLockUpsertion(key)) {
            LOGGER.trace(
                "Still waiting for the lock of the key '{}' for threadId '{}' ({} of {})... ",
                key, threadId(), currentTimeMillis() - waitStartedTime, timeoutMs
            )
            try {
                sleep(Random().nextInt(opts.waitIntervalMs.toInt()).toLong())
            } catch (e: InterruptedException) {
                throw LockWaitTimeoutException("Timeout loop has been interrupted!", e)
            }
            if (currentTimeMillis() - waitStartedTime > timeoutMs) {
                throw LockWaitTimeoutException("Lock wait timed out for key '$key'!")
            }
        }
        LOGGER.trace(
            "Locked successfully for key '{}' for threadId '{}'...",
            key,
            threadId()
        )
    }

    @Throws(InvalidLockOwnerException::class)
    override fun unlock(key: String) {
        LOGGER.trace("Unlocking key '{}' for threadId '{}'...", key, threadId())
        if (collection().deleteOne(byIdMine(key)).deletedCount == 0L) {
            throw InvalidLockOwnerException(
                "Current thread '" + threadId() + "' is not the owner of the lock " +
                        "for key '" + key + "'!"
            )
        }
        LOGGER.trace(
            "Unlocked successfully for key '{}' for threadId '{}'...",
            key,
            threadId()
        )
    }

    override fun forceUnlock(key: String) {
        LOGGER.trace(
            "Forcing unlock of the key '{}' for threadId '{}'...",
            key,
            threadId()
        )
        collection().deleteOne(byId(key))
    }

    override fun isLocked(key: String): Boolean {
        return collection().find(byId(key)).limit(1).iterator().hasNext()
    }

    override fun isLockedByMe(key: String): Boolean {
        return collection().find(byIdMine(key)).limit(1).iterator().hasNext()
    }

    private fun byId(key: String): Document {
        return Document("_id", key)
    }

    private fun byIdMine(key: String): Document {
        return Document()
            .append("_id", key)
            .append("threadId", threadId())
    }

    private fun tryLockUpsertion(key: String): Boolean {
        return try {
            collection().updateOne(
                byId(key),
                Document(
                    "\$setOnInsert",
                    Document(
                        mapOf(
                            "_id" to key,
                            "threadId" to threadId(),
                            "lockedSince" to currentTimeMillis()
                        )
                    )
                ),
                UpdateOptions().upsert(true)
            ).upsertedId != null
        } catch (e: MongoWriteException) {
            LOGGER.trace(
                "Failed to upsert lock value into {} for {}",
                collection,
                threadId(),
                e
            )
            false
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(MongoLockingSupport::class.java)
    }
}
