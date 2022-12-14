package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.ClusterAware.Heartbeat
import io.github.smecsia.poreia.core.api.ClusterAware.Role.PRIMARY
import io.github.smecsia.poreia.core.api.ClusterAware.Role.REPLICA
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.ScheduledJob
import io.github.smecsia.poreia.core.api.Scheduler
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.greaterThan
import org.hamcrest.Matchers.greaterThanOrEqualTo
import org.hamcrest.Matchers.lessThanOrEqualTo
import org.junit.Ignore
import org.junit.Test
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger

class BasicSchedulerTest {
    @Test
    @Ignore("flaky - figure out why")
    fun testStartMasterAndSlave() {
        val repo = BasicRepoBuilder<Heartbeat>().build("test", Opts(maxLockWaitMs = 100))
        val lockKey = "TEST_LOCK_KEY"
        val schedulers = (1..2).map {
            BasicScheduler("$it", repo, maxNoHBMs = 500, hbIntervalMs = 500, lockKey = lockKey)
                .also { sleep(300) }
        }
        assertThat(schedulers[0].role, equalTo(PRIMARY))
        val seq100ms = AtomicInteger()
        val seq200ms = AtomicInteger()
        schedulers.forEach { s: Scheduler ->
            s.addJob(
                ScheduledJob(
                    name = "global",
                    frequency = Duration.ofMillis(100),
                    task = { seq100ms.incrementAndGet() },
                ),
                global = true,
            )
            s.addJob(
                ScheduledJob(
                    name = "local",
                    frequency = Duration.ofMillis(200),
                    task = { seq200ms.incrementAndGet() },
                ),
                global = false,
            )
            s.start()
            sleep(200)
        }
        sleep(1300)
        await().atMost(2, SECONDS).until({ seq100ms.get() }, allOf(greaterThan(10), lessThanOrEqualTo(20)))
        await().atMost(2, SECONDS).until({ seq200ms.get() }, allOf(greaterThan(10), lessThanOrEqualTo(20)))
        assertThat(schedulers[0].role, equalTo(PRIMARY))
        schedulers[0].apply {
            terminate()
            sleep(1000)
            start()
        }
        sleep(1000)
        assertThat(seq100ms.get(), greaterThanOrEqualTo(20))
        assertThat(seq200ms.get(), greaterThanOrEqualTo(20))
        // trying to force re-election
        await().atMost(10, SECONDS).until({
            repo[lockKey] = Heartbeat(0, "unknown")
            sleep(50)
            schedulers[0].role
        }, { it == PRIMARY },)
        await().atMost(2, SECONDS).until({ schedulers[1].role }, { it == REPLICA })
        await().atMost(2, SECONDS).until({ seq100ms.get() }, greaterThanOrEqualTo(30))
        await().atMost(2, SECONDS).until({ seq200ms.get() }, greaterThanOrEqualTo(30))
    }

    @Test
    fun `it should parse schedule correctly and start jobs immediately`() {
        val repo = BasicRepoBuilder<Heartbeat>().build("test", Opts(maxLockWaitMs = 100))
        val scheduler = BasicScheduler("1", repo, maxInitialDelayMs = 1)
        var executedCounter = 0
        scheduler.addJob(
            ScheduledJob(name = "one", schedule = "every 30 mins", task = {
                executedCounter++
            },),
        )
        scheduler.addJob(ScheduledJob(name = "one", schedule = "every day 01:00", task = { }))
        scheduler.addJob(ScheduledJob(name = "one", schedule = "every monday 12:00", task = { }))
        scheduler.start()

        await().atMost(2, SECONDS).until({ executedCounter }, equalTo(1))
    }
}
