package io.github.smecsia.poreia.core.api

/**
 * Builder for scheduler instance
 */
interface SchedulerBuilder {
    fun build(name: String, opts: Opts = Opts()): Scheduler
}

typealias SchedulerBuilderFnc = (name: String, opts: Opts) -> Scheduler
