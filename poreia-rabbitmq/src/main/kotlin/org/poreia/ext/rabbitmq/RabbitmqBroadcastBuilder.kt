package org.poreia.ext.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType.FANOUT
import com.rabbitmq.client.Connection
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.BroadcastBuilder
import org.poreia.core.api.queue.Broadcaster
import org.poreia.core.api.serialize.ToBytesSerializer

class RabbitmqBroadcastBuilder<M>(
    connection: Connection,
    serializer: ToBytesSerializer<M>,
    private val queueOpts: RabbitmqOpts = RabbitmqOpts()
) : AbstractRabbitmqBuilder<M>(connection, serializer), BroadcastBuilder<M> {

    override fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        val exchangeName = "bc-$target"
        val channel = createChannel()
        val dRes = channel.queueDeclare("", true, true, true, null)
        channel.exchangeDeclare(exchangeName, FANOUT)
        channel.queueBind(dRes.queue, exchangeName, "")
        return RabbitmqBroadcaster(target, channel, pipeline, serializer, opts,
            exchangeName = exchangeName, queueName = dRes.queue)
    }
}
