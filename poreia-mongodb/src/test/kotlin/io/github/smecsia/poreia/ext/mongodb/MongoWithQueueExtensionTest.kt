package io.github.smecsia.poreia.ext.mongodb

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.ext.mongodb.core.MongoTailableCursorQueue.QueueOpts
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.awaitility.Awaitility.await
import org.junit.Rule
import org.junit.Test
import java.io.Serializable
import java.util.concurrent.TimeUnit.SECONDS

class MongoWithQueueExtensionTest {

    @Rule
    @JvmField
    var mongodb = MongoDbRule()

    @Test
    fun `broadcasters and queues should work across multiple pipelines`() {
        val p1 = testPipeline("p1", 2)
        val p2 = testPipeline("p2", 3)

        listOf(p1, p2).forEach { it.send("input", Msg(value = 1, from = "root")) }

        await().atMost(5, SECONDS).until(
            { p1.repo("output")["output"]?.value },
            { it == 18 },
        )

        p1.stop()
        p2.stop()
    }

    companion object {
        data class Msg(var value: Int = 0, var from: String = "") : Serializable
        data class Value(var value: Int = 0)
    }

    private fun testPipeline(name: String, inc: Int): io.github.smecsia.poreia.core.Pipeline<Msg, Value> {
        return startPipeline(name) {
            stateInit = { Value() }
            bcBuilder = io.github.smecsia.poreia.ext.mongodb.MongoBroadcastBuilder(
                mongodb.client,
                "test",
                queueOpts = QueueOpts(maxSize = 100, maxDocSize = 100),
            )
            queueBuilder = MongoQueueBuilder(mongodb.client, "test")
            repoBuilder = MongoRepoBuilder(mongodb.client, "test")

            process(
                "input",
                process = { m -> Msg(m.value + 1, from = name) },
            ).output("forward")

            process("forward").broadcast("broadcasted")

            process("broadcasted", process = { m -> Msg(m.value + inc, from = name) })
                .output("output")

            aggregateTo("output", aggregate = { s, m -> s.value += m.value }, opts = Opts(consumers = 1))
        }
    }
}
