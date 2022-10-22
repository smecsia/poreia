package org.poreia.core.impl

import org.poreia.core.api.ClusterAware.Heartbeat
import org.poreia.core.api.Opts
import org.poreia.core.api.Scheduler
import org.poreia.core.api.SchedulerBuilder
import org.poreia.core.api.ThreadPoolBuilder

class BasicSchedulerBuilder(
    private val repoBuilder: BasicRepoBuilder<Heartbeat>,
    private val threadPoolBuilder: ThreadPoolBuilder = BasicThreadPoolBuilder()
) : SchedulerBuilder {
    override fun build(name: String, opts: Opts): Scheduler {
        return BasicScheduler(name,
            repo = repoBuilder.build(name, opts),
            threadPoolBuilder = threadPoolBuilder
        )
    }
}
