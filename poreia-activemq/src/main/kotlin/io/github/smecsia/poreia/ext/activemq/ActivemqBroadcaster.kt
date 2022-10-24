package io.github.smecsia.poreia.ext.activemq

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import java.lang.Thread.sleep
import java.util.Random
import javax.jms.ConnectionFactory
import javax.jms.Destination
import javax.jms.IllegalStateException
import javax.jms.Session

class ActivemqBroadcaster<M> @JvmOverloads constructor(
    destName: String,
    factory: ConnectionFactory,
    pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>,
    serializer: ToBytesSerializer<M>,
    opts: Opts = Opts(),
) : AbstractActivemqConsumer<M>(destName, factory, serializer, opts),
    Broadcaster<M> {

    private val threadpool = pipeline.threadPoolBuilder.build(opts.consumers)
    private val consumers = mutableListOf<ActivemqConsumer<M>>()

    override fun broadcast(message: M) {
        produce(message)
    }

    override fun terminate() {
        try {
            consumers.forEach { it.terminate() }
            threadpool.shutdownNow()
        } catch (e: Exception) {
            LOGGER.warn("Failed to terminate consumers or thread pool", e)
        }
    }

    override fun initDestination(session: Session, name: String): Destination {
        return session.createTopic(name)
    }

    init {
        LOGGER.debug("[${pipeline.name}][$destName] Starting ${opts.consumers} broadcaster consumers...")
        val random = Random()
        repeat(opts.consumers) { idx: Int ->
            val consumerId = "[${pipeline.name}][$destName#$idx]"
            LOGGER.debug("$consumerId Starting broadcaster consumer...")
            val consumer = newConsumer()
            consumers.add(consumer)
            threadpool.submit {
                while (true) {
                    try {
                        consumer.consume { msg ->
                            LOGGER.debug("$consumerId got event $msg")
                            pipeline.send(destName, msg)
                        }
                    } catch (e: IllegalStateException) {
                        LOGGER.debug("$consumerId broadcaster state error", e)
                        sleep(random.nextInt(1000).toLong())
                    } catch (e: Exception) {
                        if (threadpool.isShutdown()) {
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
