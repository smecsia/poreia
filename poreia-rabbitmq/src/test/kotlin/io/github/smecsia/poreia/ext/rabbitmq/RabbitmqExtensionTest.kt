package io.github.smecsia.poreia.ext.rabbitmq

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.impl.BasicSerializer
import io.github.smecsia.poreia.core.inMemoryRepo
import io.github.smecsia.poreia.core.singletonRepoBuilder
import io.github.smecsia.poreia.ext.rabbitmq.util.RabbitmqRule
import org.awaitility.Awaitility.await
import org.junit.Rule
import org.junit.Test
import java.io.Serializable
import java.util.concurrent.TimeUnit.SECONDS
import java.util.function.Predicate.isEqual

class RabbitmqExtensionTest {

    @Rule
    @JvmField
    var rabbitmq = RabbitmqRule()
    private val repo = inMemoryRepo { Value() }

    @Test
    fun `broadcasters and queues should work across multiple pipelines`() {
        val p1 = startPipeline<Msg, Value>("p1") {
            baseConfig()

            aggregateTo("output", aggregate = { s, m -> s.value += m.value + 2 }, opts = Opts(consumers = 1))
        }

        val p2 = startPipeline<Msg, Value>("p2") {
            baseConfig()

            aggregateTo("output", aggregate = { s, m -> s.value += m.value + 3 }, opts = Opts(consumers = 1))
        }

        listOf(p1, p2).forEach { it.send("input", Msg(value = 1, from = "root")) }

        await().atMost(10, SECONDS).until(
            { p1.repo("output")["output"]?.value },
            isEqual(18),
        )
        listOf(p1, p2).forEach { it.stop() }
    }

    companion object {
        data class Msg(var value: Int = 0, var from: String = "") : Serializable
        data class Value(var value: Int = 0)
    }

    private fun io.github.smecsia.poreia.core.Pipeline<Msg, Value>.baseConfig() {
        bcBuilder = RabbitmqBroadcastBuilder(rabbitmq.connection, serializer = BasicSerializer())
        queueBuilder = RabbitmqQueueBuilder(rabbitmq.connection, serializer = BasicSerializer())
        repoBuilder = singletonRepoBuilder(repo)

        process("input", process = { m -> Msg(m.value + 1, from = name) })
            .output("forward")

        process("forward").broadcast("output")
    }
}
