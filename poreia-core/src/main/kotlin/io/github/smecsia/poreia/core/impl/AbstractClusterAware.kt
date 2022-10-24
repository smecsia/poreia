package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.ClusterAware
import io.github.smecsia.poreia.core.api.ClusterAware.Heartbeat
import io.github.smecsia.poreia.core.api.ClusterAware.Role
import io.github.smecsia.poreia.core.api.ClusterAware.Role.PRIMARY
import io.github.smecsia.poreia.core.api.ClusterAware.Role.REPLICA
import io.github.smecsia.poreia.core.api.ThreadPoolBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import org.slf4j.LoggerFactory
import java.lang.System.currentTimeMillis
import java.lang.Thread.sleep

abstract class AbstractClusterAware(
    override val name: String,
    private val repo: Repository<Heartbeat>,
    threadPoolBuilder: ThreadPoolBuilder = BasicThreadPoolBuilder(),
    private val lockKey: String = LOCK_KEY,
    private val maxNoHBMs: Long = MAX_NO_HB_INTERVAL_MS.toLong(),
    private val hbIntervalMs: Int = HB_INTERVAL_MS,
) : ClusterAware {
    private val threadPool = threadPoolBuilder.build(1)

    @Volatile
    private var terminated = false

    @Volatile
    final override var role: Role = PRIMARY
        private set

    override fun start() {
        role = REPLICA
        logger.debug("${logPrefix()} Starting node")
        terminated = false
        threadPool.submit {
            initReplicaActivity()
            logger.debug("${logPrefix()} Waiting for primary lock")
            try {
                if (becomePrimary()) {
                    primaryLoop()
                } else {
                    restart()
                }
            } catch (e: InterruptedException) {
                logger.error("${logPrefix()} Failed to become primary", e)
            }
        }
    }

    protected open fun initReplicaActivity() {}
    protected open fun initPrimaryActivity() {}
    protected fun logPrefix() = "[$name][$role]"

    override fun restart() {
        logger.warn("${logPrefix()} Restarting!")
        role = REPLICA
        suspend()
        sleep(hbIntervalMs.toLong())
        start()
    }

    override fun suspend() {
        if (repo.isLockedByMe(lockKey)) {
            repo.unlock(lockKey)
        }
    }

    override fun terminate(): Boolean {
        logger.warn("${logPrefix()} Terminating node!")
        return true.also { suspend(); terminated = true }
    }

    @Throws(InterruptedException::class)
    private fun becomePrimary(): Boolean {
        role = REPLICA
        while (!terminated) {
            try {
                repo.lock(lockKey)
                role = PRIMARY
                return true
            } catch (ignored: LockWaitTimeoutException) {
                logger.debug("${logPrefix()} Still waiting for primary node to release the lock...")
                val lastHb = lastHb()
                val currentTimeMillis = currentTimeMillis()
                if (lastHb < currentTimeMillis - maxNoHBMs) {
                    logger.warn("${logPrefix()} Last heartbeat is older than ${maxNoHBMs}ms ($lastHb < $currentTimeMillis - $maxNoHBMs), forcing unlock")
                    repo.forceUnlock(lockKey)
                }
            } catch (e: Exception) {
                logger.error("${logPrefix()} Error while waiting for primary node to release the lock", e)
                sleep(hbIntervalMs.toLong())
            }
        }
        role = REPLICA
        return false
    }

    private fun primaryLoop() {
        try {
            logger.info("${logPrefix()} Now I am PRIMARY")
            writeHeartbeat()
            initPrimaryActivity()
            logger.debug("${logPrefix()} Starting primary loop")
            role = PRIMARY
            while (!terminated && repo.isLockedByMe(lockKey)) {
                writeHeartbeat()
                sleep(hbIntervalMs.toLong())
            }
        } catch (e: Exception) {
            logger.error("${logPrefix()} Primary loop has been terminated due to error", e)
        }
        logger.warn("${logPrefix()} Primary loop has ended, restarting...")
        if (repo.isLockedByMe(lockKey)) {
            repo.unlock(lockKey)
        }
        if (!terminated) {
            restart()
        } else {
            logger.warn("${logPrefix()} Terminated")
        }
    }

    private fun writeHeartbeat() {
        repo[lockKey] = newHeartbeat()
    }

    private fun lastHb(): Long {
        val timer: Heartbeat? = try {
            repo[lockKey]
        } catch (e: InterruptedException) {
            return 0
        }
        return timer!!.lastUpdated
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractClusterAware::class.java)
        const val LOCK_KEY = "CLUSTER"
        const val HB_INTERVAL_MS = 5000
        const val MAX_NO_HB_INTERVAL_MS = 10000
    }
}
