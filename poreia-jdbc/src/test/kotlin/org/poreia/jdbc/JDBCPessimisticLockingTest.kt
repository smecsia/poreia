package org.poreia.jdbc

import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.poreia.core.error.LockWaitTimeoutException
import java.lang.Thread.sleep
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(Parameterized::class)
class JDBCPessimisticLockingTest : BaseJDBCRepoTest() {

    @Test
    fun `it should lock and unlock`() {
        val locking = JDBCPessimisticLocking(
            connection = jdbcRepoBuilder.connection,
            tableName = "locks",
            maxLockWaitMs = 10,
            dialect = jdbcRepoBuilder.dialect
        )
        val key = "key1"
        locking.tryLock(key, 300)
        locking.tryLock(key, 300)
        assertThat(locking.isLockedByMe(key), equalTo(true))
        val failedToLock = AtomicBoolean(false)
        Thread {
            try {
                locking.tryLock(key, 200)
            } catch (e: LockWaitTimeoutException) {
                failedToLock.set(true)
            }
        }.start()
        sleep(300)
        assertThat(failedToLock.get(), equalTo(true))
        failedToLock.set(false)
        val lockedByThread = AtomicBoolean(false)
        locking.unlock(key)
        Thread {
            try {
                locking.tryLock(key, 200)
                lockedByThread.set(locking.isLockedByMe(key))
            } catch (e: LockWaitTimeoutException) {
                failedToLock.set(true)
            }
        }.start()
        sleep(300)
        assertThat(locking.isLocked(key), equalTo(true))
        assertThat(failedToLock.get(), equalTo(false))
        assertThat(lockedByThread.get(), equalTo(true))
    }
}