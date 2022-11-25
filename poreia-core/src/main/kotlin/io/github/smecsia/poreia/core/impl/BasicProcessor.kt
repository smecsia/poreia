package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.ProcessingStrategy
import io.github.smecsia.poreia.core.api.processing.Filter
import io.github.smecsia.poreia.core.api.processing.ProcessingException
import io.github.smecsia.poreia.core.api.processing.Processor
import io.github.smecsia.poreia.core.api.processing.ProcessorBuilder
import io.github.smecsia.poreia.core.api.queue.QueueConsumer
import org.slf4j.LoggerFactory
import java.util.function.Consumer

open class BasicProcessor<M>(
    val pipeline: Pipeline<M, *>,
    override val name: String,
    private val strategy: ProcessingStrategy<M>,
    private var filter: Filter<M>? = null,
    val opts: Opts = Opts(),
) : Processor<M> {
    private val outputs: MutableList<Consumer<M>> = mutableListOf()

    @Volatile
    var stopped = false

    protected open fun processNext(message: M, consumerName: String): M {
        return strategy.process(message)
    }

    protected fun stop(): Boolean {
        return true.also { stopped = it }
    }

    override fun run(consumer: QueueConsumer<M>, consumerName: String) {
        while (!stopped) {
            LOG.trace("[{}][{}#{}] blocking on consumer...", pipeline.name, name, consumerName)
            var msg: M? = null
            try {
                consumer.consume { message ->
                    msg = message
                    LOG.debug("[{}][{}#{}] processing message {}", pipeline.name, name, consumerName, message)
                    if (filter?.filter(message) != false) {
                        val outEvent = processNext(message, consumerName)
                        outputs.forEach(Consumer { out: Consumer<M> -> out.accept(outEvent) })
                    } else {
                        LOG.debug(
                            "[{}][{}#{}] message filtered out: {}",
                            pipeline.name,
                            name,
                            consumerName,
                            message,
                        )
                    }
                }
            } catch (e: ProcessingException) {
                LOG.error(
                    "[{}][{}#{}] failed to filter/process message {}: {}",
                    pipeline.name,
                    name,
                    consumerName,
                    msg,
                    e.message,
                    e,
                )
            }
        }
    }

    override fun output(vararg targets: String): BasicProcessor<M> {
        pipeline.route.append(" *-> ${targets.joinToString(",")}")
        targets.forEach { target: String ->
            outputs.add(Consumer { message: M -> pipeline.send(target, message) })
        }
        return this
    }

    override fun broadcast(vararg targets: String): BasicProcessor<M> {
        pipeline.route.append(" >>> ${targets.joinToString(",")}")
        targets.forEach { target: String ->
            outputs.add(Consumer { message: M -> pipeline.broadcast(target, message) })
        }
        return this
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(BasicProcessor::class.java)

        class Builder<M> : ProcessorBuilder<M> {
            override fun build(
                pipeline: Pipeline<M, *>,
                name: String,
                strategy: ProcessingStrategy<M>,
                filter: Filter<M>?,
                opts: Opts,
            ): Processor<M> = BasicProcessor<M>(
                pipeline,
                name,
                strategy,
                filter,
                opts,
            )
        }
    }
}
