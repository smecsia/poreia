package org.poreia.core.api

interface SchedulerBuilder {
    fun build(name: String, opts: Opts = Opts()): Scheduler
}

typealias SchedulerBuilderFnc = (name: String, opts: Opts) -> Scheduler
