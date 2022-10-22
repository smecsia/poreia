package org.poreia.ext.gcloud

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import io.grpc.ManagedChannelBuilder
import org.awaitility.Awaitility.await
import org.junit.Rule
import org.junit.Test
import org.poreia.core.Pipeline
import org.poreia.core.Pipeline.Companion.startPipeline
import org.poreia.core.api.Opts
import org.poreia.core.inMemoryRepo
import org.poreia.core.singletonRepoBuilder
import org.poreia.ext.gcloud.util.PubSubRule
import java.io.Serializable
import java.util.concurrent.TimeUnit.SECONDS

class PubSubExtensionTest {

    @Rule
    @JvmField
    var pubsub = PubSubRule()
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

        listOf(p1, p2).forEach {
            it.send("input", Msg(value = 1, from = "root"))
        }

        await().atMost(5, SECONDS).until(
            { p1.repo("output")["output"]?.value },
            { it == 18 }
        )
        listOf(p1, p2).forEach { it.stop() }

        p1.stop()
        p2.stop()
    }

    companion object {
        data class Msg(var value: Int = 0, var from: String = "") : Serializable
        data class Value(var value: Int = 0)
    }

    private fun Pipeline<Msg, Value>.baseConfig() {
        val channel = ManagedChannelBuilder.forTarget(pubsub.endpoint).usePlaintext().build()
        val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        val credsProvider = NoCredentialsProvider.create()
        defaultOpts = Opts(consumers = 3)
        bcBuilder = PubSubBroadcastBuilder(
            "project",
            channelProvider = channelProvider,
            credentialsProvider = credsProvider
        )
        queueBuilder = PubSubQueueBuilder(
            "project",
            channelProvider = channelProvider,
            credentialsProvider = credsProvider
        )
        repoBuilder = singletonRepoBuilder(repo)

        process("input", process = { m -> Msg(m.value + 1, from = name) })
            .output("forward")

        process("forward").broadcast("output")
    }
}
