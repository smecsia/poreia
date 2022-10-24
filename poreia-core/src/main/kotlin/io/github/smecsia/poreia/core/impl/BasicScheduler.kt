package io.github.smecsia.poreia.core.impl

import com.github.shyiko.skedule.Schedule
import io.github.smecsia.poreia.core.api.ClusterAware.Heartbeat
import io.github.smecsia.poreia.core.api.ScheduledJob
import io.github.smecsia.poreia.core.api.Scheduler
import io.github.smecsia.poreia.core.api.ThreadPoolBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.ZonedDateTime.now
import java.util.Random
import java.util.Timer
import java.util.TimerTask
import java.util.function.Consumer

class BasicScheduler @JvmOverloads constructor(
    name: String,
    repo: Repository<Heartbeat>,
    threadPoolBuilder: ThreadPoolBuilder = BasicThreadPoolBuilder(),
    maxNoHBMs: Long = MAX_NO_HB_INTERVAL_MS.toLong(),
    hbIntervalMs: Int = HB_INTERVAL_MS,
    lockKey: String = LOCK_KEY,
    private val maxInitialDelayMs: Int = MAX_INITIAL_DELAY_MS,
) : Scheduler, AbstractClusterAware(
    name,
    repo = repo,
    threadPoolBuilder = threadPoolBuilder,
    lockKey = lockKey,
    maxNoHBMs = maxNoHBMs,
    hbIntervalMs = hbIntervalMs,
) {
    private val globalJobs: MutableList<ScheduledJob> = ArrayList()
    private val localJobs: MutableList<ScheduledJob> = ArrayList()
    private val startedTimers: MutableList<Timer> = ArrayList()

    override fun addJob(job: ScheduledJob, global: Boolean) {
        logger.debug(
            "${logPrefix()} Registering ${if (global) "global" else "local"} job ${job.name} occurring ${job.schedule}",
        )
        if (global) {
            globalJobs.add(job)
        } else {
            localJobs.add(job)
        }
    }

    override fun removeJobs() {
        localJobs.clear()
        globalJobs.clear()
    }

    override fun initReplicaActivity() {
        logger.debug("${logPrefix()} Starting local jobs")
        startJobs(localJobs)
    }

    override fun initPrimaryActivity() {
        logger.debug("${logPrefix()} Starting global jobs")
        startJobs(globalJobs)
    }

    override fun suspend() {
        logger.warn("${logPrefix()} Suspending all started jobs!")
        startedTimers.forEach(Consumer { obj: Timer -> obj.cancel() })
        startedTimers.clear()
    }

    private fun startJobs(jobs: Collection<ScheduledJob>) {
        try {
            jobs.forEach(
                Consumer { job: ScheduledJob ->
                    var freqMs: Long? = null
                    var delayMs: Long = Random().nextInt(maxInitialDelayMs).toLong()

                    job.schedule?.let {
                        val schedule = Schedule.parse(it)
                        val nextExec = schedule.nextOrSame(now())
                        val nextAfterNext = schedule.next(nextExec)
                        freqMs = nextAfterNext.toInstant().toEpochMilli() - nextExec.toInstant().toEpochMilli()
                        delayMs = nextExec.toInstant().toEpochMilli() - now().toInstant().toEpochMilli()
                    }
                    job.frequency?.let {
                        freqMs = it.toMillis()
                    }
                    if (delayMs < 0) {
                        delayMs = 0
                    }
                    logger.info(
                        "${logPrefix()} Scheduling job \"${job.name}\" to run every ${Duration.ofMillis(freqMs ?: 0)}ms " +
                            "with initial delay of ${Duration.ofMillis(delayMs)}ms ",
                    )
                    val timer = Timer()
                    timer.schedule(
                        object : TimerTask() {
                            override fun run() {
                                logger.debug("${logPrefix()} Executing job '${job.name}'...")
                                try {
                                    job.task()
                                } catch (e: Exception) {
                                    logger.error("${logPrefix()} Failed to execute job '${job.name}'", e)
                                }
                            }
                        },
                        delayMs,
                        freqMs ?: throw IllegalStateException("${job.name}: no schedule/frequency configured!"),
                    )
                    startedTimers.add(timer)
                },
            )
        } catch (e: Exception) {
            logger.error("${logPrefix()} Failed to start jobs", e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BasicScheduler::class.java)
        const val LOCK_KEY = "Scheduler"
        const val HB_INTERVAL_MS = 5000
        const val MAX_NO_HB_INTERVAL_MS = 10000
        const val MAX_INITIAL_DELAY_MS = 100
    }
}
