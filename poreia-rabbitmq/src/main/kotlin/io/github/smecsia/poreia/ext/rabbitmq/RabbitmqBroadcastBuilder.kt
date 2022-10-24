package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.BuiltinExchangeType.FANOUT
import com.rabbitmq.client.Connection
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

class RabbitmqBroadcastBuilder<M>(
    connection: Connection,
    serializer: ToBytesSerializer<M>,
    private val queueOpts: RabbitmqOpts = RabbitmqOpts(),
) : AbstractRabbitmqBuilder<M>(connection, serializer), BroadcastBuilder<M> {

    override fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        val exchangeName = "bc-$target"
        val channel = createChannel()
        val dRes = channel.queueDeclare("", true, true, true, null)
        channel.exchangeDeclare(exchangeName, FANOUT)
        channel.queueBind(dRes.queue, exchangeName, "")
        return RabbitmqBroadcaster(
            target,
            channel,
            pipeline,
            serializer,
            opts,
            exchangeName = exchangeName,
            queueName = dRes.queue,
        )
    }
}
