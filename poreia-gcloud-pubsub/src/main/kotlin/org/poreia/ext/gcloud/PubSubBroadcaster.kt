package org.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Broadcaster
import org.poreia.core.api.serialize.ToBytesSerializer
import java.util.Random
import java.util.concurrent.RejectedExecutionException

class PubSubBroadcaster<M> @JvmOverloads constructor(
    projectId: String,
    channelProvider: TransportChannelProvider? = null,
    credentialsProvider: CredentialsProvider? = null,
    topicId: String,
    pipeline: Pipeline<M, *>,
    serializer: ToBytesSerializer<M>,
    opts: Opts = Opts()
) : AbstractPubSubQueue<M>(projectId, channelProvider, credentialsProvider, "bc-${topicId}", serializer, opts),
    Broadcaster<M> {

    private val threadpool = pipeline.threadPoolBuilder.build(opts.consumers)
    private val consumers = mutableListOf<PubSubConsumer<M>>()

    override fun broadcast(message: M) {
        publishMessage(message)
    }

    override fun terminate() {
        try {
            consumers.forEach { it.terminate() }
            threadpool.shutdownNow()
        } catch (e: Exception) {
            logger.warn("Failed to terminate consumers or thread pool", e)
        }
    }

    // TODO: refactoring: extract common code of ActivemqBroadcaster and PubSubBroadcaster to a common place
    init {
        logger.debug("[${pipeline.name}][$topicId] Starting ${opts.consumers} broadcaster consumers...")
        val random = Random()
        repeat(opts.consumers) { idx: Int ->
            val consumerId = "[${pipeline.name}][$topicId#$idx]"
            logger.debug("$consumerId Starting broadcaster consumer...")
            val subscriptionId = "p${pipeline.name}-d$topicId-b$idx"
            val consumer = initConsumer(subscriptionId)
            consumers.add(consumer)
            threadpool.submit {
                while (!threadpool.isShutdown()) {
                    try {
                        consumer.consume { msg ->
                            logger.debug("$consumerId got event $msg")
                            pipeline.send(topicId, msg)
                        }
                    } catch (e: IllegalStateException) {
                        logger.debug("$consumerId broadcaster state error", e)
                        Thread.sleep(random.nextInt(1000).toLong())
                    } catch (e: Exception) {
                        if(threadpool.isShutdown()) {
                            logger.trace("$consumerId thread pool interrupted", e)
                            break
                        }
                        logger.error("$consumerId failed to consume message", e)
                    }
                }
            }
        }
    }


}
