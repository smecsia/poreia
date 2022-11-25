package io.github.smecsia.poreia.core.api

typealias Task = () -> Unit

interface Scheduler : ClusterAware {
    fun addJob(job: ScheduledJob, global: Boolean = true)

    fun removeJobs()
}