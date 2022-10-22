package org.poreia.core.api.processing

import org.poreia.core.error.LockWaitTimeoutException

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
}
