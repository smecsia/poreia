package io.github.smecsia.poreia.ext.mongodb

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.ext.mongodb.core.MongoTailableCursorQueue
import org.slf4j.LoggerFactory
import java.util.function.Consumer

class MongoBroadcaster<M : Any>(
    private val target: String,
    private val pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>,
    private val queue: MongoTailableCursorQueue<M>,
    opts: Opts = pipeline.defaultOpts,
) : Broadcaster<M> {
    private val threadpool = pipeline.threadPoolBuilder.build(opts.consumers)

    override fun broadcast(message: M) {
        queue.add(message)
    }

    override fun terminate() {
        threadpool.shutdownNow()
    }

    init {
        io.github.smecsia.poreia.ext.mongodb.MongoBroadcaster.Companion.LOGGER.debug("[${pipeline.name}][$target] Starting ${opts.consumers} consumers...")
        repeat(opts.consumers) { idx ->
            io.github.smecsia.poreia.ext.mongodb.MongoBroadcaster.Companion.LOGGER.debug("[${pipeline.name}][$target#$idx] Starting consumer...")
            threadpool.submit {
                queue.poll(
                    Consumer { msg ->
                        io.github.smecsia.poreia.ext.mongodb.MongoBroadcaster.Companion.LOGGER.debug("[${pipeline.name}][$target#$idx] got event $msg")
                        pipeline.send(target, msg)
                    },
                )
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(io.github.smecsia.poreia.ext.mongodb.MongoBroadcaster::class.java)
    }
}
