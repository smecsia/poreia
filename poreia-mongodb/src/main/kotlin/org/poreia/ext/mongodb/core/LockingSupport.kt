package org.poreia.ext.mongodb.core

import org.poreia.core.api.processing.Locker
import org.poreia.core.error.InvalidLockOwnerException
import org.poreia.core.error.LockWaitTimeoutException

interface LockingSupport : Locker {
    val opts: LockOpts

    @Throws(LockWaitTimeoutException::class)
    fun tryLock(key: String, timeoutMs: Long)

    fun isLocked(key: String): Boolean

    @Throws(InvalidLockOwnerException::class)
    override fun unlock(key: String)

    override fun forceUnlock(key: String)

    override fun isLockedByMe(key: String): Boolean

    override fun lock(key: String) {
        tryLock(key, opts.lockTimeoutMs)
    }

    override fun tryLock(key: String): Boolean {
        return try {
            lock(key)
            true
        } catch (e: LockWaitTimeoutException) {
            false
        }
    }

    data class LockOpts(
        val lockTimeoutMs: Long = Long.MAX_VALUE, // unlimited by default
        val waitIntervalMs: Long = 100
    )
}
