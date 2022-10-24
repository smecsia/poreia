package io.github.smecsia.poreia.ext.activemq

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

class ActivemqQueueBuilder<M>(
    factory: ConnectionFactory,
    serializer: ToBytesSerializer<M>,
) : AbstractActivemqBuilder<M>(factory, serializer),
    QueueBuilder<M, Queue<M>> {
    override fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): ActivemqQueue<M> {
        return ActivemqQueue(name, factory, serializer, opts)
    }
}
