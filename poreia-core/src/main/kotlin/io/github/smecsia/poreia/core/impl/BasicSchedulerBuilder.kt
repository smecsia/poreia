package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.ClusterAware.Heartbeat
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.Scheduler
import io.github.smecsia.poreia.core.api.SchedulerBuilder
import io.github.smecsia.poreia.core.api.ThreadPoolBuilder

class BasicSchedulerBuilder(
    private val repoBuilder: BasicRepoBuilder<Heartbeat>,
    private val threadPoolBuilder: ThreadPoolBuilder = BasicThreadPoolBuilder(),
) : SchedulerBuilder {
    override fun build(name: String, opts: Opts): Scheduler {
        return BasicScheduler(
            name,
            repo = repoBuilder.build(name, opts),
            threadPoolBuilder = threadPoolBuilder,
        )
    }
}
