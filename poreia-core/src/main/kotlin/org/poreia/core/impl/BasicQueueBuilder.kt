package org.poreia.core.impl

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.queue.QueueBuilder

class BasicQueueBuilder<M> :
    QueueBuilder<M, Queue<M>> {
    override fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): BasicBlockingQueue<M> {
        return BasicBlockingQueue(name, opts.maxQueueSize)
    }
}
