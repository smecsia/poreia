package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Connection
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

class RabbitmqQueueBuilder<M>(
    connection: Connection,
    serializer: ToBytesSerializer<M>,
    private val queueOpts: RabbitmqOpts = RabbitmqOpts(),
) : AbstractRabbitmqBuilder<M>(connection, serializer), QueueBuilder<M, Queue<M>> {

    override fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): RabbitmqQueue<M> {
        val queueName = "q-${pipeline.name}-$name"
        val channel = createChannel()
        channel.queueDeclare(queueName, queueOpts.durable, queueOpts.exclusive, queueOpts.autoDelete, queueOpts.args)
        channel.exchangeDeclare(queueName, BuiltinExchangeType.DIRECT)
        channel.queueBind(queueName, queueName, "")
        return RabbitmqQueue(queueName, channel, serializer, opts = queueOpts, commonOpts = opts)
    }
}
