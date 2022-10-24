package io.github.smecsia.poreia.core.api.queue

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts

interface QueueBuilder<M, Q : Queue<M>> {
    fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Q
}
