package io.github.smecsia.poreia.core

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argThat
import com.nhaarman.mockitokotlin2.atLeastOnce
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.given
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.ProcessingStrategy
import io.github.smecsia.poreia.core.api.Scheduler
import io.github.smecsia.poreia.core.api.processing.AggregationKey
import io.github.smecsia.poreia.core.api.processing.AggregationStrategy
import io.github.smecsia.poreia.core.api.processing.Aggregator
import io.github.smecsia.poreia.core.api.processing.AggregatorBuilder
import io.github.smecsia.poreia.core.api.processing.Filter
import io.github.smecsia.poreia.core.api.processing.Processor
import io.github.smecsia.poreia.core.api.processing.ProcessorBuilder
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import org.awaitility.Awaitility.await
import org.hamcrest.CoreMatchers.hasItem
import org.hamcrest.CoreMatchers.sameInstance
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThanOrEqualTo
import org.hamcrest.collection.IsCollectionWithSize.hasSize
import org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder
import org.hamcrest.collection.IsIterableContainingInOrder.contains
import org.junit.Test
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS

@Suppress("UNCHECKED_CAST")
class PipelineTest {

    @Test
    fun `simple pipeline simply works`() {
        val pipeline = simplePipeline {
            process(
                "input",
                process = { it.plus(mapOf("processed" to true)) },
            ).output("all")

            aggregateTo(
                "all",
                key = { "all" },
                aggregate = { state, msg ->
                    val messages: MutableList<SimpleMessage> =
                        state.getOrPut("messages") { mutableListOf<SimpleMessage>() } as MutableList<SimpleMessage>
                    messages.add(msg)
                },
            )
        }

        repeat(3) {
            pipeline.send("input", mutableMapOf("key" to "message#$it"))
        }

        await().atMost(2, SECONDS).until({ pipeline.repo("all").keys() }, hasItem("all"))
        val state = pipeline.repo("all")["all"] ?: mutableMapOf()
        val events = state["messages"] as MutableList<SimpleMessage>
        assertThat(events, hasSize(3))
        assertThat(events.map { it["processed"] as Boolean }, contains(true, true, true))
    }

    @Test
    fun `pipeline works with data classes and filter`() {
        data class Message(val name: String)
        data class User(var fullName: String = "")

        val pipeline = startPipeline<Message, User> {
            stateInit = { User() }
            process(
                "input",
                filter = { it.name != "Vasya" },
                process = { Message(name = "${it.name} Johnson") },
            ).output("users")
            aggregateTo(
                "users",
                key = { it.name },
                aggregate = { user, msg ->
                    user.fullName = msg.name
                },
            )
        }

        listOf("Petya", "Vasya", "Poheru", "Malina").forEach {
            pipeline.send("input", Message(name = it))
        }
        await().atMost(2, SECONDS).until(
            { pipeline.repo("users").keys() },
            containsInAnyOrder("Petya Johnson", "Poheru Johnson", "Malina Johnson"),
        )
    }

    @Test
    fun `pipeline works with scheduler`() {
        val pipeline = basicPipeline {
            aggregateTo("input", key = { it["id"]!! }, aggregate = { s, _ -> s["ticks"] = "0" })
            schedule(name = "tick", frequency = Duration.ofMillis(300), task = {
                repo("input").withEach { _, s ->
                    s["ticks"] = "${(s["ticks"]?.toInt() ?: 0) + 1}"
                }
            },)
        }
        pipeline.send("input", mutableMapOf("id" to "hello"))
        pipeline.send("input", mutableMapOf("id" to "timers"))

        await().atMost(5, SECONDS).until({ pipeline.repo("input").keys() }, containsInAnyOrder("hello", "timers"))

        sleep(1000)
        val state1 = pipeline.repo("input")["hello"] as State<String>
        val state2 = pipeline.repo("input")["timers"] as State<String>
        assertThat(state1["ticks"]?.toInt() ?: 0, greaterThanOrEqualTo(3))
        assertThat(state2["ticks"]?.toInt() ?: 0, greaterThanOrEqualTo(3))
    }

    @Test
    fun `pipeline uses custom scheduler`() {
        val scheduler = mock<Scheduler> {}
        basicPipeline {
            schedulerBuilder = { _, _ -> scheduler }
            schedule(frequency = Duration.ofMillis(300), task = {}, name = "global", global = true)
            schedule(frequency = Duration.ofSeconds(100), task = {}, name = "local", global = false)
        }
        verify(scheduler).addJob(
            job = argThat { name == "global" && frequency == Duration.ofMillis(300) },
            global = eq(true),
        )
        verify(scheduler).addJob(
            job = argThat { name == "local" && frequency == Duration.ofSeconds(100) },
            global = eq(false),
        )
    }

    @Test
    fun `pipeline allows overriding builders per aggregator`() {
        val bcBuilder = mock<BroadcastBuilder<Message<String>>> {}
        val queueBuilder = mock<QueueBuilder<Message<String>, Queue<Message<String>>>> {}
        val repository = mock<Repository<State<String>>> { }
        val aggregator = mock<Aggregator<Message<String>, State<String>>> {}
        val aggregatorBuilder = TestAggregatorBuilder(aggregator)
        given(queueBuilder.build(any(), any(), any())).willReturn(mock { })
        val repoBuilder = TestRepoBuilder(repository)
        given(aggregator.repository).willReturn(repository)
        given(aggregator.name).willReturn("test")
        val pipeline = basicPipeline {
            aggregateTo(
                name = "test",
                key = { "" },
                aggregate = { _, _ -> },
                repoBuilder = repoBuilder,
                bcBuilder = bcBuilder,
                queueBuilder = queueBuilder,
                aggregatorBuilder = aggregatorBuilder,
            )
        }
        verify(queueBuilder).build(eq("test"), same(pipeline), same(pipeline.defaultOpts))
        verify(bcBuilder).build(eq("test"), same(pipeline), same(pipeline.defaultOpts))
        verify(aggregator, atLeastOnce()).name
        assertThat(pipeline.repo("test"), sameInstance(repository))
    }

    @Test
    fun `pipeline allows overriding builders per processor`() {
        val bcBuilder = mock<BroadcastBuilder<Message<String>>> {}
        val queueBuilder = mock<QueueBuilder<Message<String>, Queue<Message<String>>>> {}
        val processor = mock<Processor<Message<String>>> {}
        given(queueBuilder.build(any(), any(), any())).willReturn(mock {})
        given(processor.name).willReturn("test")
        val processorBuilder = TestProcessorBuilder(processor)
        val pipeline = basicPipeline {
            process(
                name = "test",
                process = { it },
                bcBuilder = bcBuilder,
                queueBuilder = queueBuilder,
                processorBuilder = processorBuilder,
            )
        }
        verify(queueBuilder).build(eq("test"), same(pipeline), eq(pipeline.defaultOpts))
        verify(bcBuilder).build(eq("test"), same(pipeline), eq(pipeline.defaultOpts))
        verify(processor, atLeastOnce()).name
    }

    class TestAggregatorBuilder<M, S>(private val aggregator: Aggregator<M, S>) :
        AggregatorBuilder<M, S> {
        override fun build(
            pipeline: io.github.smecsia.poreia.core.Pipeline<M, S>,
            name: String,
            strategy: AggregationStrategy<M, S>,
            key: AggregationKey<M>,
            repository: Repository<S>,
            filter: Filter<M>?,
            opts: Opts,
        ): Aggregator<M, S> = aggregator
    }

    class TestProcessorBuilder<M>(private val processor: Processor<M>) :
        ProcessorBuilder<M> {
        override fun build(
            pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>,
            name: String,
            strategy: ProcessingStrategy<M>,
            filter: Filter<M>?,
            opts: Opts,
        ): Processor<M> = processor
    }

    class TestRepoBuilder<S>(private val repo: Repository<S>) :
        RepoBuilder<S> {
        override fun build(
            name: String,
            opts: Opts,
            stateInit: StateInitializer<S>?,
            stateClass: Class<S>?,
        ): Repository<S> = repo
    }
}
