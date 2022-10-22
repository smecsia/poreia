package org.poreia.ext.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import org.poreia.core.api.serialize.ToBytesSerializer

abstract class AbstractRabbitmqBuilder<M>(
    protected val connection: Connection,
    protected val serializer: ToBytesSerializer<M>
) {
    protected fun createChannel() = connection.createChannel()
}
