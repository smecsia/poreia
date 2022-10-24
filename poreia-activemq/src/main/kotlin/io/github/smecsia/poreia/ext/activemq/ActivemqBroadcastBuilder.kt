package io.github.smecsia.poreia.ext.activemq

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

class ActivemqBroadcastBuilder<M>(
    factory: ConnectionFactory,
    serializer: ToBytesSerializer<M>,
) : AbstractActivemqBuilder<M>(factory, serializer), BroadcastBuilder<M> {

    override fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        return ActivemqBroadcaster(target, factory, pipeline, serializer, opts)
    }
}
