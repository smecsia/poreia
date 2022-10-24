package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder

class BasicQueueBuilder<M> :
    QueueBuilder<M, Queue<M>> {
    override fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): BasicBlockingQueue<M> {
        return BasicBlockingQueue(name, opts.maxQueueSize)
    }
}
