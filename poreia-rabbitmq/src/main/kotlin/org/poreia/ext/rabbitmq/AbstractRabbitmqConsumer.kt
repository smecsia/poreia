package org.poreia.ext.rabbitmq

import com.rabbitmq.client.Channel
import org.poreia.core.api.Opts
import org.poreia.core.api.serialize.ToBytesSerializer

abstract class AbstractRabbitmqConsumer<M>(
    protected val name: String,
    protected val channel: Channel,
    protected val serializer: ToBytesSerializer<M>,
    protected val opts: Opts
)
