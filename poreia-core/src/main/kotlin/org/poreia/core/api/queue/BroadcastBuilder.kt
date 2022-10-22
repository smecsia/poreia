package org.poreia.core.api.queue

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts

interface BroadcastBuilder<M> {
    fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M>
}
