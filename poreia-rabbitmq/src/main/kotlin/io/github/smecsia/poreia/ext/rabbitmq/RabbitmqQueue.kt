package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.Channel
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

/**
 * RabbitMQ implementation for the [Queue]
 */
class RabbitmqQueue<M> @JvmOverloads constructor(
    private val queueName: String,
    private val channel: Channel,
    private val serializer: ToBytesSerializer<M>,
    private val opts: RabbitmqOpts = RabbitmqOpts(),
    private val commonOpts: Opts = Opts(),
) : Queue<M> {

    override fun add(message: M) {
        channel.basicPublish(queueName, "", null, serializer.serialize(message))
    }

    override fun buildConsumer(name: String): RabbitmqConsumer<M> {
        return RabbitmqConsumer(channel, queueName, serializer, commonOpts)
    }

    override fun isEmpty(): Boolean = count() == 0

    override fun count(): Int = channel.messageCount(queueName).toInt()
}
