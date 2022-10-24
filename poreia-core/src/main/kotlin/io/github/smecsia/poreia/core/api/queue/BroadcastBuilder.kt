package io.github.smecsia.poreia.core.api.queue

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts

interface BroadcastBuilder<M> {
    fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M>
}
