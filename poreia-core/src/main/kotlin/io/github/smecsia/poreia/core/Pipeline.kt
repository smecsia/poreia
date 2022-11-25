package io.github.smecsia.poreia.core

import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.ProcessAndStopFnc
import io.github.smecsia.poreia.core.api.ProcessingFnc
import io.github.smecsia.poreia.core.api.ScheduledJob
import io.github.smecsia.poreia.core.api.Scheduler
import io.github.smecsia.poreia.core.api.SchedulerBuilderFnc
import io.github.smecsia.poreia.core.api.Task
import io.github.smecsia.poreia.core.api.ThreadPool
import io.github.smecsia.poreia.core.api.ThreadPoolBuilder
import io.github.smecsia.poreia.core.api.processing.AggregationFnc
import io.github.smecsia.poreia.core.api.processing.AggregationKeyFnc
import io.github.smecsia.poreia.core.api.processing.Aggregator
import io.github.smecsia.poreia.core.api.processing.AggregatorBuilder
import io.github.smecsia.poreia.core.api.processing.FilterFnc
import io.github.smecsia.poreia.core.api.processing.Processor
import io.github.smecsia.poreia.core.api.processing.ProcessorBuilder
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.api.processing.aggregate
import io.github.smecsia.poreia.core.api.processing.filter
import io.github.smecsia.poreia.core.api.processing.key
import io.github.smecsia.poreia.core.api.processing.process
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import io.github.smecsia.poreia.core.api.queue.QueueConsumer
import io.github.smecsia.poreia.core.error.InvalidRouteException
import io.github.smecsia.poreia.core.impl.BasicAggregator
import io.github.smecsia.poreia.core.impl.BasicBroadcastBuilder
import io.github.smecsia.poreia.core.impl.BasicProcessor
import io.github.smecsia.poreia.core.impl.BasicQueueBuilder
import io.github.smecsia.poreia.core.impl.BasicRepoBuilder
import io.github.smecsia.poreia.core.impl.BasicSchedulerBuilder
import io.github.smecsia.poreia.core.impl.BasicThreadPoolBuilder
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.time.Duration
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Main class of the framework, so-called "Pipeline".
 * Holds the whole state of all objects operating on the pipeline. Allows to build up the routes and set-up
 * queue consumers and producers.
 */
open class Pipeline<M, S>(
    val name: String,
) {
    val route = StringBuilder()
    private val processors: MutableMap<String, Processor<M>> = mutableMapOf()
    private val threadpools: MutableMap<String, MutableList<ThreadPool>> = mutableMapOf()
    private val queues: MutableMap<String, Queue<M>> = mutableMapOf()
    private val broadcasters: MutableMap<String, Broadcaster<M>> = mutableMapOf()
    private val opts: MutableMap<String, Opts> = mutableMapOf()
    private val onStopCallbacks: MutableSet<() -> Unit> = mutableSetOf()
    private val onStartCallbacks: MutableSet<() -> Unit> = mutableSetOf()

    @Volatile
    private var awaitLatch = CountDownLatch(1)

    private val started = AtomicBoolean(false)
    val isStarted: Boolean get() = started.get()

    @Volatile
    private var scheduler: Scheduler? = null
    private val consumers = mutableListOf<QueueConsumer<M>>()

    var processorBuilder: ProcessorBuilder<M> = BasicProcessor.Companion.Builder()
    var aggregatorBuilder: AggregatorBuilder<M, S> = BasicAggregator.Companion.Builder()
    var queueBuilder: QueueBuilder<M, Queue<M>> = BasicQueueBuilder()
    var repoBuilder: RepoBuilder<S> = BasicRepoBuilder()
    var bcBuilder: BroadcastBuilder<M> = BasicBroadcastBuilder()
    var threadPoolBuilder: ThreadPoolBuilder = BasicThreadPoolBuilder()
    var schedulerBuilder: SchedulerBuilderFnc = { n, o ->
        BasicSchedulerBuilder(
            repoBuilder = BasicRepoBuilder(),
            threadPoolBuilder = threadPoolBuilder,
        ).build(n, o)
    }
    var stateInit: StateInitializer<S>? = null
    var stateClass: Class<S>? = null
    var defaultOpts = Opts()

    @Synchronized
    fun start() {
        if (!started.get()) {
            LOG.info("[$name] Starting pipeline: \n $route")
            onStartCallbacks.forEach { it.invoke() }
            if (stateInit == null || stateClass == null) {
                LOG.warn("No state initializer / class found! Aggregators may not work normally")
            }
            scheduler?.start()
            consumers.clear()
            processors.values.forEach { p ->
                val tCount = opts(p.name).consumers
                LOG.info("[$name][${p.name}] Starting $tCount consumers...")
                val tPool = threadPoolBuilder.build(tCount)
                repeat(tCount) { idx ->
                    LOG.debug("[$name][${p.name}#$idx] Starting consumer...")
                    val consumerName = "p$name-t${p.name}-c$idx"
                    tPool.submit {
                        while (!tPool.isShutdown()) {
                            try {
                                val consumer = getQueue(p.name).buildConsumer(consumerName)
                                consumers.add(consumer)
                                p.run(consumer, consumerName)
                            } catch (e: RejectedExecutionException) {
                                LOG.debug("[$name][${p.name}#$idx] Consumer exited due to thread pool shutdown: ${e.message}")
                                break
                            } catch (e: Exception) {
                                if (tPool.isShutdown() || !started.get()) {
                                    LOG.debug("[$name][${p.name}#$idx] Consumer exited due to thread pool shutdown: ${e.message}")
                                    break
                                }
                                val delay = Random().nextInt(5000).toLong()
                                LOG.error(
                                    "[$name][${p.name}#$idx] Consumer exited with exception, restart in ${delay}ms",
                                    e,
                                )
                                sleep(delay)
                            }
                        }
                    }
                }
                addThreadPool(p.name, tPool)
            }
            awaitLatch = CountDownLatch(1)
            started.set(true)
        }
    }

    @Synchronized
    fun stop() {
        if (started.get()) {
            LOG.info("[$name] Stopping pipeline")
            onStopCallbacks.forEach { it.invoke() }
            scheduler?.terminate()
            consumers.forEach { it.terminate() }
            queues.forEach { (_, q) ->
                q.terminate()
            }
            broadcasters.forEach { (_, b) ->
                b.terminate()
            }
            threadpools.forEach { (_, tps) ->
                tps.forEach { it.shutdownNow() }
            }
            started.set(false)
            awaitLatch.countDown()
        }
    }

    fun await() {
        awaitLatch.await()
    }

    // ------------------
    // DSL

    fun send(target: String, message: M) {
        getQueue(target).add(message)
    }

    fun broadcast(name: String, message: M) {
        getOrCreateBroadcaster(name).broadcast(message)
    }

    @Synchronized
    fun aggregateTo(
        name: String,
        aggregate: (S, M) -> Unit,
        key: AggregationKeyFnc<M> = { name },
        filter: FilterFnc<M>? = null,
        opts: Opts = defaultOpts,
        queueBuilder: QueueBuilder<M, Queue<M>> = this.queueBuilder,
        bcBuilder: BroadcastBuilder<M> = this.bcBuilder,
        repoBuilder: RepoBuilder<S> = this.repoBuilder,
        aggregatorBuilder: AggregatorBuilder<M, S> = this.aggregatorBuilder,
    ): Processor<M> = aggregate(
        name = name,
        aggregate = { s, m -> aggregate(s, m); m },
        key = key,
        filter = filter,
        opts = opts,
        queueBuilder = queueBuilder,
        bcBuilder = bcBuilder,
        repoBuilder = repoBuilder,
        aggregatorBuilder = aggregatorBuilder,
    )

    @Synchronized
    fun aggregate(
        name: String,
        aggregate: AggregationFnc<S, M>,
        key: AggregationKeyFnc<M> = { name },
        filter: FilterFnc<M>? = null,
        opts: Opts = defaultOpts,
        queueBuilder: QueueBuilder<M, Queue<M>> = this.queueBuilder,
        bcBuilder: BroadcastBuilder<M> = this.bcBuilder,
        repoBuilder: RepoBuilder<S> = this.repoBuilder,
        aggregatorBuilder: AggregatorBuilder<M, S> = this.aggregatorBuilder,
    ): Processor<M> {
        route.append("-> agg:$name")
        this.broadcasters[name] = getOrCreateBroadcaster(name, opts, bcBuilder = bcBuilder)
        this.opts[name] = opts
        this.queues[name] = buildQueue(name, opts, queueBuilder = queueBuilder)
        val aggregator = aggregatorBuilder.build(
            name = name,
            pipeline = this,
            filter = filter?.let { filter(it) },
            key = key { key(it) },
            strategy = aggregate { s, m -> aggregate(s, m) },
            repository = initRepository(repoBuilder, name, opts),
            opts = opts,
        )
        this.processors[name] = aggregator
        return aggregator
    }

    @Synchronized
    fun processWithLock(
        name: String,
        process: ProcessingFnc<M>,
        key: AggregationKeyFnc<M> = { name },
        filter: FilterFnc<M>? = null,
        opts: Opts = defaultOpts,
        queueBuilder: QueueBuilder<M, Queue<M>> = this.queueBuilder,
        bcBuilder: BroadcastBuilder<M> = this.bcBuilder,
        repoBuilder: RepoBuilder<S> = this.repoBuilder,
        aggregatorBuilder: AggregatorBuilder<M, S> = this.aggregatorBuilder,
    ): Processor<M> = aggregate(
        name = name,
        aggregate = { _, m -> process(m) },
        key = key,
        filter = filter,
        opts = opts,
        queueBuilder = queueBuilder,
        bcBuilder = bcBuilder,
        repoBuilder = repoBuilder,
        aggregatorBuilder = aggregatorBuilder,
    )

    @Synchronized
    fun process(
        name: String,
        process: ProcessingFnc<M> = { it },
        filter: FilterFnc<M>? = null,
        opts: Opts = defaultOpts,
        queueBuilder: QueueBuilder<M, Queue<M>> = this.queueBuilder,
        bcBuilder: BroadcastBuilder<M> = this.bcBuilder,
        processorBuilder: ProcessorBuilder<M> = this.processorBuilder,
    ): Processor<M> {
        route.append("-> proc:$name")
        this.broadcasters[name] = getOrCreateBroadcaster(name, bcBuilder = bcBuilder)
        this.opts[name] = opts
        this.queues[name] = buildQueue(name, opts, queueBuilder = queueBuilder)
        val processor = processorBuilder.build(
            name = name,
            pipeline = this,
            filter = filter?.let { filter(it) },
            strategy = process { process(it) },
            opts = opts,
        )
        this.processors[name] = processor
        return processor
    }

    fun schedule(
        task: Task,
        frequency: Duration? = null,
        schedule: String? = null,
        name: String = "",
        global: Boolean = true,
        opts: Opts = defaultOpts,
    ) {
        getOrInitScheduler(opts).addJob(
            ScheduledJob(
                name = name,
                schedule = schedule,
                frequency = frequency,
                task = task,
            ),
            global = global,
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun repo(name: String): Repository<S> {
        return when (processors[name]) {
            is Aggregator<*, *> -> (processors[name] as Aggregator<M, S>).repository
            else -> throw IllegalArgumentException("Repository '$name' not found!")
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun allRepos(): Collection<Repository<S>> = processors.values
        .filter { it is Aggregator<*, *> }
        .map { (it as Aggregator<M, S>).repository }

    fun queue(name: String): Queue<M>? = queues[name]

    fun queueNames(): Set<String> = queues.keys

    fun processAndPass(process: ProcessAndStopFnc<M>): ProcessingFnc<M> = { m -> process(m); m }

    fun onStart(callback: () -> Unit) = onStartCallbacks.add(callback)

    fun onStop(callback: () -> Unit) = onStopCallbacks.add(callback)

    // ------------------
    // PRIVATE

    @Synchronized
    private fun getOrCreateBroadcaster(
        target: String,
        opts: Opts = defaultOpts.copy(consumers = 1),
        bcBuilder: BroadcastBuilder<M> = this.bcBuilder,
    ): Broadcaster<M> {
        return broadcasters.getOrPut(target) {
            bcBuilder.build(target, this, opts)
        }
    }

    @Synchronized
    private fun getOrInitScheduler(opts: Opts = defaultOpts): Scheduler {
        if (scheduler == null) {
            scheduler = schedulerBuilder.invoke("$name-scheduler", opts)
        }
        return scheduler!!
    }

    private fun buildQueue(
        name: String,
        opts: Opts = defaultOpts,
        queueBuilder: QueueBuilder<M, Queue<M>> = this.queueBuilder,
    ): Queue<M> {
        return queueBuilder.build(name, this, opts)
    }

    @Synchronized
    private fun opts(target: String): Opts {
        return opts.getOrPut(target) { defaultOpts }
    }

    @Synchronized
    private fun addThreadPool(name: String, tPool: ThreadPool) {
        threadpools.getOrPut(name) { mutableListOf() }
        threadpools[name]!!.add(tPool)
    }

    private fun getQueue(name: String): Queue<M> {
        return queues[name]
            ?: throw InvalidRouteException("Processor '$name' not initialized for pipeline '${this.name}'!")
    }

    private fun initRepository(repoBuilder: RepoBuilder<S>, name: String, opts: Opts) =
        repoBuilder.build(name, opts, stateInit, stateClass)

    fun shutdown() {
        threadpools.values.flatten().forEach {
            it.shutdownNow()
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(Pipeline::class.java)

        fun <M, S> startPipeline(name: String = "pipeline", definition: Pipeline<M, S>.() -> Unit): Pipeline<M, S> =
            Pipeline<M, S>(name).also(definition).also { it.start() }

        fun <M, S> pipeline(name: String = "pipeline", definition: Pipeline<M, S>.() -> Unit): Pipeline<M, S> =
            Pipeline<M, S>(name).also(definition)
    }
}
