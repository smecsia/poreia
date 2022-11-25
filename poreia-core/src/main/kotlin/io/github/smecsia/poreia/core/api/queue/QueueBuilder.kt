package io.github.smecsia.poreia.core.api.queue

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts

/**
 * Queue builder allows to build the queue based on its name and current pipeline instance
 */
interface QueueBuilder<M, Q : Queue<M>> {
    /**
     * Builds queue object for the queue with the specific name
     * @param name name of the queue
     * @param pipeline current pipeline instance
     */
    fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): Q
}
