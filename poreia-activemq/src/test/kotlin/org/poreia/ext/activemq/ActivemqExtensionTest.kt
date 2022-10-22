package org.poreia.ext.activemq

import org.awaitility.Awaitility.await
import org.junit.Rule
import org.junit.Test
import org.poreia.core.Pipeline
import org.poreia.core.Pipeline.Companion.startPipeline
import org.poreia.core.api.Opts
import org.poreia.core.impl.BasicSerializer
import org.poreia.core.inMemoryRepo
import org.poreia.core.singletonRepoBuilder
import org.poreia.ext.activemq.util.ActivemqRule
import java.io.Serializable
import java.util.concurrent.TimeUnit.SECONDS

class ActivemqExtensionTest {

    @Rule
    @JvmField
    var activemq = ActivemqRule()
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

        await().atMost(5, SECONDS).until(
            { p1.repo("output")["output"]?.value },
            { it == 18 }
        )

        p1.stop()
        p2.stop()
    }

    companion object {
        data class Msg(var value: Int = 0, var from: String = "") : Serializable
        data class Value(var value: Int = 0)
    }

    private fun Pipeline<Msg, Value>.baseConfig() {
        bcBuilder = ActivemqBroadcastBuilder(factory = activemq.factory, serializer = BasicSerializer())
        queueBuilder = ActivemqQueueBuilder(factory = activemq.factory, serializer = BasicSerializer())
        repoBuilder = singletonRepoBuilder(repo)

        process("input", process = { m -> Msg(m.value + 1, from = name) })
            .output("forward")

        process("forward").broadcast("output")
    }
}
