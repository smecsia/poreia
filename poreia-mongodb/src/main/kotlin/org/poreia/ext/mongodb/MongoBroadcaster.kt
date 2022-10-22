package org.poreia.ext.mongodb

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Broadcaster
import org.poreia.ext.mongodb.core.MongoTailableCursorQueue
import org.slf4j.LoggerFactory
import java.util.function.Consumer

class MongoBroadcaster<M : Any>(
    private val target: String,
    private val pipeline: Pipeline<M, *>,
    private val queue: MongoTailableCursorQueue<M>,
    opts: Opts = pipeline.defaultOpts
) : Broadcaster<M> {
    private val threadpool = pipeline.threadPoolBuilder.build(opts.consumers)

    override fun broadcast(message: M) {
        queue.add(message)
    }

    override fun terminate() {
        threadpool.shutdownNow()
    }

    init {
        LOGGER.debug("[${pipeline.name}][$target] Starting ${opts.consumers} consumers...")
        repeat(opts.consumers) { idx ->
            LOGGER.debug("[${pipeline.name}][$target#$idx] Starting consumer...")
            threadpool.submit {
                queue.poll(Consumer { msg ->
                    LOGGER.debug("[${pipeline.name}][$target#$idx] got event $msg")
                    pipeline.send(target, msg)
                })
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(MongoBroadcaster::class.java)
    }
}
