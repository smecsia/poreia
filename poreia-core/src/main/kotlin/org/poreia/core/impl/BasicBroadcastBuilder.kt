package org.poreia.core.impl

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.BroadcastBuilder
import org.poreia.core.api.queue.Broadcaster

class BasicBroadcastBuilder<M> : BroadcastBuilder<M> {
    override fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        return BasicBroadcaster(target, pipeline)
    }
}
