package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.State
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Assert.assertTrue
import org.junit.Test
import java.lang.Thread.sleep
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.atomic.AtomicInteger

internal class ConcurrentHashMapRepositoryTest {
    @Test
    fun testLockUnlock() {
        val repo = BasicRepoBuilder<State<Int>>().build("repo", Opts(maxLockWaitMs = 100))
        val key = "key1"
        repo.lock(key)
        repo.set(key, mutableMapOf("value" to 1))
        val counter = AtomicInteger()
        newSingleThreadExecutor().submit {
            while (true) {
                try {
                    repo.lock(key)
                    counter.incrementAndGet()
                } catch (e: LockWaitTimeoutException) {
                    println("Failed to wait for the lock: ${e.message}")
                }
                sleep(100)
            }
        }
        sleep(200)
        assertThat(counter.get()).isEqualTo(0)
        assertTrue(repo.isLockedByMe(key))
        repo.unlock(key)
        sleep(400)
        assertThat(counter.get()).isGreaterThan(0)
        assertThat(repo.isLockedByMe(key)).isFalse()
        assertThatThrownBy { repo.lock(key) }
            .hasMessageContaining("Failed to lock key $key within timeout of")
        assertThat(counter.get()).isGreaterThanOrEqualTo(1)
        assertThat(repo[key] as State<Int>).hasEntrySatisfying("value") { assertThat(it).isEqualTo(1) }
        repo.clear()
        assertThat(repo.keys()).isEmpty()
    }
}
