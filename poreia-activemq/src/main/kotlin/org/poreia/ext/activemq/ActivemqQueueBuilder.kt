package org.poreia.ext.activemq

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.queue.QueueBuilder
import org.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

class ActivemqQueueBuilder<M>(
    factory: ConnectionFactory,
    serializer: ToBytesSerializer<M>
) : AbstractActivemqBuilder<M>(factory, serializer),
    QueueBuilder<M, Queue<M>> {
    override fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): ActivemqQueue<M> {
        return ActivemqQueue(name, factory, serializer, opts)
    }
}
