package org.poreia.jdbc

import org.poreia.core.api.processing.Locker
import org.poreia.core.error.InvalidLockOwnerException
import org.poreia.core.error.LockWaitTimeoutException
import org.poreia.jdbc.dialect.BasicDialect
import org.poreia.jdbc.dialect.Dialect
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.System.currentTimeMillis
import java.sql.Connection
import java.sql.SQLException

class JDBCPessimisticLocking(
    val connection: Connection,
    private val tableName: String = DEFAULT_TABLE,
    private val maxLockWaitMs: Long = 5000,
    private val lockPollIntervalMs: Long = 10,
    private val dialect: Dialect = BasicDialect(),
) : Locker {
    init {
        dialect.createLocksTableIfNotExists(tableName, connection)
    }

    companion object {
        const val DEFAULT_TABLE = "__poreia_locks__"
        val LOGGER: Logger = LoggerFactory.getLogger(JDBCPessimisticLocking::class.java)
    }

    fun tryLock(key: String, timeoutMs: Long) {
        val lockStarted = currentTimeMillis()
        LOGGER.debug("Trying to lock key '$key'")
        while (currentTimeMillis() - lockStarted < timeoutMs) {
            try {
                if (dialect.isLockedByMe(tableName, key, connection)) {
                    LOGGER.debug("key '$key' is already locked by me")
                    return
                }
                dialect.tryLock(tableName, key, connection)
                LOGGER.debug("Successfully locked key '$key'")
                return
            } catch (e: SQLException) {
                LOGGER.trace("Lock trial was unsuccessful for key $key", e)
            }
            LOGGER.trace("Still waiting for lock '$key'...")
            Thread.sleep(lockPollIntervalMs)
        }
        throw LockWaitTimeoutException("Failed to lock key '$key' within ${timeoutMs}ms")
    }

    override fun forceUnlock(key: String) {
        dialect.forceUnlock(tableName, key, connection)
    }

    @Throws(InvalidLockOwnerException::class)
    override fun unlock(key: String) {
        try {
            LOGGER.trace("Trying to unlock key $key")
            dialect.tryUnlock(tableName, key, connection)
        } catch (e: SQLException) {
            LOGGER.debug("Failed to unlock key $key", e)
            throw InvalidLockOwnerException(msg = "Failed to unlock key '$key'", e = e)
        }
    }

    fun isLocked(key: String): Boolean {
        return dialect.isLocked(tableName, key, connection)
    }

    override fun isLockedByMe(key: String): Boolean {
        return dialect.isLockedByMe(tableName, key, connection)
    }

    override fun lock(key: String) {
        if (!tryLock(key)) {
            throw LockWaitTimeoutException("Failed to lock key $key within timeout of ${maxLockWaitMs}ms")
        }
    }

    override fun tryLock(key: String): Boolean = try {
        tryLock(key, maxLockWaitMs)
        true
    } catch (e: LockWaitTimeoutException) {
        LOGGER.debug("Failed to lock key '$key' within ${maxLockWaitMs}ms", e)
        false
    }
}