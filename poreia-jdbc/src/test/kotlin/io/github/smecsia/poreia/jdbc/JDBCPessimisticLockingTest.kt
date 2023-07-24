package io.github.smecsia.poreia.jdbc

import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import org.awaitility.Awaitility.await
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.lang.Thread.sleep
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(Parameterized::class)
class JDBCPessimisticLockingTest : BaseJDBCRepoTest() {

    @Test
    fun `it should lock and unlock`() {
        val locking = JDBCPessimisticLocking(
            connection = jdbcRepoBuilder.connection,
            tableName = "locks",
            maxLockWaitMs = 10,
            dialect = jdbcRepoBuilder.dialect,
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
        val lockedByMe = AtomicBoolean(false)

        locking.unlock(key)
        tryToLockInThread(locking, key, lockedByMe, failedToLock)
        assertLockedByMe(locking, key, failedToLock, lockedByMe)

        // force unlock all
        locking.forceUnlockAll()
        tryToLockInThread(locking, key, lockedByMe, failedToLock)
        assertLockedByMe(locking, key, failedToLock, lockedByMe)
    }

    private fun tryToLockInThread(
        locking: JDBCPessimisticLocking,
        key: String,
        lockedByThread: AtomicBoolean,
        failedToLock: AtomicBoolean,
    ) {
        failedToLock.set(false)
        lockedByThread.set(false)
        Thread {
            try {
                locking.tryLock(key, 200)
                lockedByThread.set(locking.isLockedByMe(key))
            } catch (e: LockWaitTimeoutException) {
                failedToLock.set(true)
            }
        }.start()
    }

    private fun assertLockedByMe(
        locking: JDBCPessimisticLocking,
        key: String,
        failedToLock: AtomicBoolean,
        lockedByThread: AtomicBoolean,
    ) {
        await().atMost(1, TimeUnit.SECONDS).until {
            locking.isLocked(key) && !failedToLock.get() && lockedByThread.get()
        }
        assertThat(locking.isLocked(key), equalTo(true))
        assertThat(failedToLock.get(), equalTo(false))
        assertThat(lockedByThread.get(), equalTo(true))
    }
}
