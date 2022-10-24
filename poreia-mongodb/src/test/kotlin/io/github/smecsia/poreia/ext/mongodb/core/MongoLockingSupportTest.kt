package io.github.smecsia.poreia.ext.mongodb.core

import io.github.smecsia.poreia.core.error.InvalidLockOwnerException
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import io.github.smecsia.poreia.ext.mongodb.core.LockingSupport.LockOpts
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.hamcrest.CoreMatchers
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.Rule
import org.junit.Test
import java.lang.Thread.sleep
import java.net.UnknownHostException
import java.util.concurrent.atomic.AtomicLong

class MongoLockingSupportTest {
    @Rule
    @JvmField
    var mongodb: MongoDbRule = MongoDbRule()

    @Test
    @Throws(Exception::class)
    fun testLockAndUnlock() {
        val lock = createLocking()
        lock.tryLock("someKey", 100L)
        assertThat(lock.isLocked("someKey"), `is`(true))
        assertThat(lock.isLockedByMe("someKey"), `is`(true))
        lock.unlock("someKey")
        assertThat(lock.isLocked("someKey"), `is`(false))
        assertThat(lock.isLockedByMe("someKey"), `is`(false))
        forceLockInSeparateThread("someKey")
        assertThat(lock.isLocked("someKey"), `is`(true))
        assertThat(lock.isLockedByMe("someKey"), `is`(false))
    }

    @Test
    @Throws(Exception::class)
    fun testTryLockWithOtherThread() {
        val waitedMs = AtomicLong()
        val lock = createLocking()
        lock.tryLock("key1", 100L)
        val otherThread = Thread(
            Runnable {
                val otherLock = createLocking()
                val startedWaitTime = System.currentTimeMillis()
                otherLock.tryLock("key1", 3000L)
                waitedMs.set(System.currentTimeMillis() - startedWaitTime)
            },
        )
        otherThread.start()
        sleep(1100L)
        lock.unlock("key1")
        otherThread.join()
        assertThat(
            waitedMs.get(),
            CoreMatchers.allOf(
                Matchers.greaterThan(1000L),
                Matchers.lessThan(2000L),
            ),
        )
    }

    @Test(expected = LockWaitTimeoutException::class)
    @Throws(Exception::class)
    fun testTimeoutForOtherThread() {
        forceLockInSeparateThread("someKey")
        createLocking().tryLock("someKey", 500L)
    }

    @Test(expected = InvalidLockOwnerException::class)
    @Throws(Exception::class)
    fun testInvalidLockOwner() {
        val lock = createLocking()
        lock.tryLock("key1", 100L)
        forceLockInSeparateThread("key1")
        lock.unlock("key1")
    }

    @Test
    @Throws(Exception::class)
    fun testLockingTwiceByCurrentThreadIsAllowed() {
        val lock = createLocking()
        lock.tryLock("key", 100L)
        assertThat(lock.isLockedByMe("key"), `is`(true))
        lock.tryLock("key", 100L)
        assertThat(lock.isLockedByMe("key"), `is`(true))
    }

    private fun createLocking(): MongoLockingSupport {
        return try {
            MongoLockingSupport(
                mongo = mongodb.client,
                dbName = "test",
                collection = "test",
                opts = LockOpts(
                    waitIntervalMs = 100,
                ),
            )
        } catch (e: UnknownHostException) {
            throw RuntimeException(e)
        }
    }

    @Throws(InterruptedException::class)
    private fun forceLockInSeparateThread(key: String) {
        val thread = Thread(
            Runnable {
                val lock = createLocking()
                lock.forceUnlock(key)
                lock.tryLock(key, 100L)
            },
        )
        thread.start()
        thread.join()
    }
}
