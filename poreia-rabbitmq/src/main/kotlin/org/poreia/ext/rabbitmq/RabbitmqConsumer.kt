package org.poreia.ext.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.QueueConsumer
import org.poreia.core.api.queue.QueueConsumerCallback
import org.poreia.core.api.queue.ackOnError
import org.poreia.core.api.serialize.ToBytesSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.Random
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class RabbitmqConsumer<M>(
    private val channel: Channel,
    override val queueName: String,
    private val serializer: ToBytesSerializer<M>,
    private val opts: Opts = Opts(),
    private val terminated: AtomicBoolean = AtomicBoolean(false),
) : QueueConsumer<M> {
    private val blockingQueue = ArrayBlockingQueue<Pair<Envelope, M>>(1000)
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun consume(callback: QueueConsumerCallback<M>) {
        channel.basicConsume(queueName, false, consumeToBlockingQueue())
        blockingQueue.take().let { (envelope, msg) ->
            callback.ackOnError(msg, opts.ackOnError) {
                ackResponse(envelope)
            }
        }
    }

    private fun consumeToBlockingQueue() = object : DefaultConsumer(channel) {
        override fun handleDelivery(
            consumerTag: String,
            envelope: Envelope,
            properties: AMQP.BasicProperties,
            body: ByteArray
        ) = try {
            blockingQueue.put(Pair(envelope, serializer.deserialize(body)))
        } catch (e: Exception) {
            if (opts.ackOnError) {
                ackResponse(envelope)
            }
            throw IllegalArgumentException("Failed to consume message from $queueName", e)
        }
    }

    override fun terminate() = try {
        terminated.set(true)
        channel.close()
    } catch (ignored: Exception) {
        logger.trace("Failed to close channel", ignored)
    }

    private fun ackResponse(envelope: Envelope) {
        channel.basicAck(envelope.deliveryTag, false)
    }
}
