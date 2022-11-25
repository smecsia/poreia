package io.github.smecsia.poreia.core.api

import java.util.concurrent.TimeUnit.MINUTES

/**
 * Global options allowing to configure behavior of the pipeline
 */
data class Opts(
    val maxQueueSize: Int = 1000000, // max size for the queue
    val consumers: Int = 1, // max count of queue consumer threads
    val maxLockWaitMs: Long = MINUTES.toMillis(10), // max time to wait until lock is available (max: 10 minutes)
    val maxQueueWaitSec: Long = 0, // max time to wait until queue provides a message
    val ackOnError: Boolean = false, // acknowledge in case of an error
    val ackDeadlineSec: Int = 100, // deadline for acknowledgement of the event to be sent
)
