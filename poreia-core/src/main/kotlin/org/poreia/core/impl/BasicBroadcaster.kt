package org.poreia.core.impl

import org.poreia.core.Pipeline
import org.poreia.core.api.queue.Broadcaster

class BasicBroadcaster<M>(private val target: String, private val pipeline: Pipeline<M, *>) :
    Broadcaster<M> {
    override fun broadcast(message: M) {
        pipeline.send(target, message)
    }
}
