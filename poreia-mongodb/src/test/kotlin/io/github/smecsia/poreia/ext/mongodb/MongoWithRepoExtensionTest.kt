package io.github.smecsia.poreia.ext.mongodb

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.awaitility.Awaitility.await
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.core.Is.`is`
import org.junit.Rule
import org.junit.Test
import java.io.Serializable
import java.util.concurrent.TimeUnit.SECONDS

class MongoWithRepoExtensionTest {

    @Rule
    @JvmField
    var mongodb: MongoDbRule = MongoDbRule()

    @Test
    fun `broadcasters and repository should work across multiple pipelines`() {
        val p1 = startPipeline<Message, Value> {
            baseConfig()

            aggregateTo("output", aggregate = { s, m -> s.value += (m as Value).value + 2 })
        }

        val p2 = startPipeline<Message, Value> {
            baseConfig()

            aggregateTo("output", aggregate = { s, m -> s.value += (m as Value).value + 3 })
        }

        listOf(p1, p2).forEach { it.send("input", Value(1)) }

        await().atMost(2, SECONDS).until(
            { p1.repo("output")["output"]?.value },
            `is`(18),
        )
        assertThat(
            p1.repo("output")["output"]?.value,
            equalTo(
                p2.repo("output")["output"]?.value,
            ),
        )

        p1.stop()
        p2.stop()
    }

    companion object {
        interface Message : Serializable
        data class Value(var value: Int = 0) : Message
    }

    private fun io.github.smecsia.poreia.core.Pipeline<Message, Value>.baseConfig() {
        repoBuilder = MongoRepoBuilder(mongodb.client, "test")
        bcBuilder = io.github.smecsia.poreia.ext.mongodb.MongoBroadcastBuilder(mongodb.client, "test")
        stateInit = { Value() }
        stateClass = Value::class.java

        process("input", process = { m -> Value((m as Value).value + 1) })
            .broadcast("output")
    }
}
