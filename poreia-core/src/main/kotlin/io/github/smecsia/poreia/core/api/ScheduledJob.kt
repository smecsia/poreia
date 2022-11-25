package io.github.smecsia.poreia.core.api

import java.time.Duration

/**
 * Instance of work to perform on the given schedule
 */
data class ScheduledJob(
    val name: String,
    val task: Task,
    val schedule: String? = null,
    val frequency: Duration? = null,
)
