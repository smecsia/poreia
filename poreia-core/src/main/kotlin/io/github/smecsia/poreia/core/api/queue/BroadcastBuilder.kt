package io.github.smecsia.poreia.core.api.queue

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts

/**
 * Builder for broadcaster instances
 */
interface BroadcastBuilder<M> {
    fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M>
}
