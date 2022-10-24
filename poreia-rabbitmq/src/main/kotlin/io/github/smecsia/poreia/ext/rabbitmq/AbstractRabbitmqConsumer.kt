package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.Channel
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

abstract class AbstractRabbitmqConsumer<M>(
    protected val name: String,
    protected val channel: Channel,
    protected val serializer: ToBytesSerializer<M>,
    protected val opts: Opts,
)
