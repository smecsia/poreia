package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.processing.Locker
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit.MILLISECONDS

class ConcurrentHashMapLocker(private val opts: Opts = Opts()) : Locker {
    private val locks: MutableMap<String, Pair<Semaphore, Thread?>> = ConcurrentHashMap()

    override fun isLockedByMe(key: String): Boolean {
        return Thread.currentThread() == locks[key]?.second
    }

    @Throws(InterruptedException::class)
    override fun lock(key: String) {
        if (!tryLock(key)) {
            throw LockWaitTimeoutException("Failed to lock key $key within timeout of ${opts.maxLockWaitMs}ms")
        }
    }

    override fun forceUnlock(key: String) {
        logger.info("forceUnlock key '$key'")
        locks.computeIfPresent(key) { _, pair -> pair.first.release(); null }
    }

    override fun forceUnlockAll() {
        locks.clear()
    }

    private fun getLock(key: String) = locks.computeIfAbsent(key) {
        Pair(Semaphore(1), null)
    }.first

    @Throws(InterruptedException::class)
    override fun tryLock(key: String): Boolean {
        logger.trace("Trying to lock key '$key'")
        getLock(key).tryAcquire(opts.maxLockWaitMs, MILLISECONDS).takeIf { it }?.let {
            locks[key] = locks[key]!!.copy(second = Thread.currentThread())
            logger.trace("Key has been locked successfully '$key'")
            return true
        }
        return false
    }

    @Synchronized
    override fun unlock(key: String) {
        try {
            logger.trace("Unlocking key '$key'")
            locks.computeIfPresent(key) { _, pair ->
                if (pair.second != Thread.currentThread()) {
                    throw IllegalStateException("Key '$key' is locked by another thread (${pair.second})")
                }
                pair.first.release()
                Pair(pair.first, null)
            }
        } catch (e: Exception) {
            logger.warn("Failed to unlock key '$key'", e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConcurrentHashMapLocker::class.java)
    }
}
