package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.Connection
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

abstract class AbstractRabbitmqBuilder<M>(
    protected val connection: Connection,
    protected val serializer: ToBytesSerializer<M>,
) {
    protected fun createChannel() = connection.createChannel()
}
