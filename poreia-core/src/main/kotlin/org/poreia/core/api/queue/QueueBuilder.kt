package org.poreia.core.api.queue

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts

interface QueueBuilder<M, Q : Queue<M>> {
    fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): Q
}
