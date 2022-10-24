package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.queue.Broadcaster

class BasicBroadcaster<M>(private val target: String, private val pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>) :
    Broadcaster<M> {
    override fun broadcast(message: M) {
        pipeline.send(target, message)
    }
}
