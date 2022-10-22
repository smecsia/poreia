package org.poreia.ext.activemq

import org.poreia.core.api.Opts
import org.poreia.core.api.queue.QueueConsumer
import org.poreia.core.api.queue.QueueConsumerCallback
import org.poreia.core.api.queue.ackOnError
import org.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.BytesMessage
import javax.jms.MessageConsumer
import javax.jms.TextMessage

class ActivemqConsumer<M>(
    private val consumer: MessageConsumer,
    private val serializer: ToBytesSerializer<M>,
    override val queueName: String,
    private val opts: Opts
) : QueueConsumer<M> {
    override fun terminate() {
        consumer.close()
    }

    override fun consume(callback: QueueConsumerCallback<M>) {
        val rawMessage = (if (opts.maxQueueWaitSec > 0) consumer.receive(opts.maxQueueWaitSec) else consumer.receive())
        val bytes = when (rawMessage) {
            is BytesMessage -> {
                val bytes = ByteArray(rawMessage.bodyLength.toInt())
                rawMessage.readBytes(bytes)
                bytes
            }
            is TextMessage -> rawMessage.text.toByteArray()
            null -> return // skip null messages
            else -> throw NotImplementedError("Unsupported message type: $rawMessage")
        }
        val deserializedMsg = serializer.deserialize(bytes)
        callback.ackOnError(deserializedMsg, ackOnError = opts.ackOnError) {
            rawMessage.acknowledge()
        }
    }
}
