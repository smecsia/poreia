package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster

class BasicBroadcastBuilder<M> : BroadcastBuilder<M> {
    override fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        return BasicBroadcaster(target, pipeline)
    }
}
