package org.poreia.core.impl

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.processing.AggregationKey
import org.poreia.core.api.processing.AggregationStrategy
import org.poreia.core.api.processing.Aggregator
import org.poreia.core.api.processing.AggregatorBuilder
import org.poreia.core.api.processing.Filter
import org.poreia.core.api.processing.Repository
import org.poreia.core.error.InvalidLockOwnerException
import org.poreia.core.error.LockWaitTimeoutException
import org.slf4j.LoggerFactory

class BasicAggregator<M, S>(
    pipeline: Pipeline<M, S>,
    name: String,
    private val strategy: AggregationStrategy<M, S>,
    private val key: AggregationKey<M>,
    override val repository: Repository<S>,
    filter: Filter<M>? = null,
    opts: Opts = Opts()
) : BasicProcessor<M>(pipeline, name, strategy = strategy, filter = filter, opts = opts),
    Aggregator<M, S> {

    override fun processNext(message: M, consumerName: String): M {
        val aggKey = key.calculate(message)
        var res = message
        try {
            LOG.debug("[{}][{}#{}] aggregating {} under key '{}'", pipeline.name, name, consumerName, message, aggKey)
            val state = repository.with(aggKey, closure = { _, s -> res = strategy.process(s, message) })
            LOG.debug(
                "[{}][{}#{}] {} stored to repo under key '{}', value='{}'",
                pipeline.name,
                name,
                consumerName,
                message,
                aggKey,
                state
            )
        } catch (e: Exception) {
            when (e) {
                is InterruptedException, is LockWaitTimeoutException, is InvalidLockOwnerException -> {
                    LOG.error(
                        "[${pipeline.name}][$name] Failed to acquire lock for $aggKey within timeout " +
                                "during processing message $message. Forcing unlock for key!"
                    )
                    repository.forceUnlock(aggKey)
                }
            }
            throw e
        }
        return res
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(BasicAggregator::class.java)

        class Builder<M, S> : AggregatorBuilder<M, S> {
            override fun build(
                pipeline: Pipeline<M, S>,
                name: String,
                strategy: AggregationStrategy<M, S>,
                key: AggregationKey<M>,
                repository: Repository<S>,
                filter: Filter<M>?,
                opts: Opts
            ): Aggregator<M, S> = BasicAggregator(
                pipeline, name, strategy, key, repository, filter, opts
            )
        }
    }
}
