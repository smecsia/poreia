package org.poreia.core.impl

import java.util.concurrent.ConcurrentHashMap
import org.poreia.core.api.processing.Locker
import org.poreia.core.api.processing.Repository
import org.poreia.core.api.processing.StateInitializer
import org.poreia.core.error.IllegalLockOwnerException
import org.poreia.core.error.LockWaitTimeoutException
import org.slf4j.LoggerFactory

class ConcurrentHashMapRepository<S>(
    override val stateInitializer: StateInitializer<S>?,
    private val locker: Locker = ConcurrentHashMapLocker()
) : Repository<S>, Locker by locker {
    override fun get(key: String): S? {
        return map[key]
    }

    @Throws(LockWaitTimeoutException::class, InterruptedException::class)
    override fun lockAndGet(key: String): S? {
        lock(key)
        return get(key).also {
            logger.trace("lockAndGet key '$key' value '$it")
        }
    }

    override fun setAndUnlock(key: String, value: S): S {
        logger.trace("setAndUnlock key '$key' value '$value'")
        map[key] = value
        unlock(key)
        return value
    }

    override fun set(key: String, value: S): S {
        logger.trace("put key '$key' value '$value")
        map[key] = value
        return value
    }

    override fun keys(): Set<String> {
        return map.keys
    }

    override fun deleteAndUnlock(key: String) {
        logger.trace("Deleting and unlocking '${key}'...")
        if (!isLockedByMe(key)) {
            throw IllegalLockOwnerException("Failed to delete entry for key '$key'")
        }
        map.remove(key)
        unlock(key)
    }

    override fun clear() {
        logger.trace("clearing the map...")
        map.clear()
    }

    private val map: MutableMap<String, S> = ConcurrentHashMap<String, S>()
    override fun values(): Map<String, S> {
        return map
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConcurrentHashMapRepository::class.java)
    }
}
