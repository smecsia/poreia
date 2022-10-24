package io.github.smecsia.poreia.ext.rabbitmq

import com.rabbitmq.client.AlreadyClosedException
import com.rabbitmq.client.Channel
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.Random
import java.util.concurrent.atomic.AtomicBoolean

class RabbitmqBroadcaster<M> @JvmOverloads constructor(
    private val destName: String,
    private val channel: Channel,
    private val pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>,
    private val serializer: ToBytesSerializer<M>,
    private val opts: Opts = Opts(),
    private val exchangeName: String = "bc-${pipeline.name}-$destName",
    private val queueName: String = destName,
) : Broadcaster<M> {

    private val threadpool = pipeline.threadPoolBuilder.build(opts.consumers)
    private val consumers = mutableListOf<RabbitmqConsumer<M>>()
    private val terminated = AtomicBoolean(false)

    override fun broadcast(message: M) = channel.basicPublish(exchangeName, "", null, serializer.serialize(message))

    override fun terminate() {
        try {
            terminated.set(true)
            consumers.forEach { it.terminate() }
            threadpool.shutdownNow()
        } catch (e: Exception) {
            LOGGER.warn("Failed to terminate consumers or thread pool", e)
        }
    }

    private val LOGGER: Logger = LoggerFactory.getLogger(javaClass)

    init {
        LOGGER.debug("[${pipeline.name}][$destName] Starting ${opts.consumers} broadcaster consumers...")
        val random = Random()
        repeat(opts.consumers) { idx: Int ->
            val consumerId = "[${pipeline.name}][$destName#$idx]"
            LOGGER.debug("$consumerId Starting broadcaster consumer...")
            val consumer = RabbitmqConsumer(channel, queueName, serializer, opts)
            consumers.add(consumer)
            threadpool.submit {
                while (!threadpool.isShutdown()) {
                    try {
                        consumer.consume { msg ->
                            LOGGER.debug("$consumerId got event $msg")
                            pipeline.send(destName, msg)
                        }
                    } catch (e: IllegalStateException) {
                        LOGGER.debug("$consumerId consumer state error", e)
                        sleep(random.nextInt(1000).toLong())
                    } catch (e: AlreadyClosedException) {
                        LOGGER.debug("$consumerId connection closed", e)
                        break
                    } catch (e: Exception) {
                        if (threadpool.isShutdown() || terminated.get()) {
                            LOGGER.trace("$consumerId thread pool interrupted", e)
                            break
                        }
                        LOGGER.error("$consumerId failed to consume message", e)
                    }
                }
            }
        }
    }
}
