package io.github.smecsia.poreia.core.api

typealias Task = () -> Unit

/**
 * Base interface for scheduled operations service
 */
interface Scheduler : ClusterAware {
    /**
     *
     */
    fun addJob(job: ScheduledJob, global: Boolean = true)

    fun removeJobs()
}
