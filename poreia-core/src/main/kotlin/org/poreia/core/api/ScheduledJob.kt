package org.poreia.core.api

import java.time.Duration

data class ScheduledJob(
    val name: String,
    val task: Task,
    val schedule: String? = null,
    val frequency: Duration? = null
)
