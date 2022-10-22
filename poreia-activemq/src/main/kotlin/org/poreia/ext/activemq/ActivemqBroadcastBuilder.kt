package org.poreia.ext.activemq

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.BroadcastBuilder
import org.poreia.core.api.queue.Broadcaster
import org.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

class ActivemqBroadcastBuilder<M>(
    factory: ConnectionFactory,
    serializer: ToBytesSerializer<M>
) : AbstractActivemqBuilder<M>(factory, serializer), BroadcastBuilder<M> {

    override fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        return ActivemqBroadcaster(target, factory, pipeline, serializer, opts)
    }
}
