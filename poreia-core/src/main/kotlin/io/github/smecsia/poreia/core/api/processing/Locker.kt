package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.error.LockWaitTimeoutException

/**
 * Base interface for locking-aware operations
 */
interface Locker {
    /**
     * Returns true if key is locked by current thread
     */
    fun isLockedByMe(key: String): Boolean

    /**
     * Locks key without getting value
     */
    @Throws(InterruptedException::class, LockWaitTimeoutException::class)
    fun lock(key: String)

    /**
     * Tries to lock key within given timeout.
     * Does not throw an exception
     *
     * @return true if lock is acquired successfully, false otherwise
     */
    @Throws(InterruptedException::class)
    fun tryLock(key: String): Boolean

    /**
     * Unlocking key
     */
    fun unlock(key: String)

    /**
     * Forcing unlock of the key
     */
    fun forceUnlock(key: String)

    /**
     * Forcing unlock of all keys
     */
    fun forceUnlockAll()
}
