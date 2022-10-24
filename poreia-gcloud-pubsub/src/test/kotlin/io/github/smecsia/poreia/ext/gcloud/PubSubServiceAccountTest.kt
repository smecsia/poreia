package io.github.smecsia.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.basicPipeline
import io.github.smecsia.poreia.core.inMemoryRepo
import io.github.smecsia.poreia.core.singletonRepoBuilder
import org.awaitility.Awaitility.await
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.FileInputStream
import java.util.concurrent.TimeUnit.SECONDS

@Ignore("This test is only for manual execution")
class PubSubServiceAccountTest {

    private val projectId = "traffic-generator-358421"
    private lateinit var credsProvider: CredentialsProvider
    private val channelProvider = SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
        .setMaxInboundMessageSize(2 * 1024) // 2KB (maximum message size).
        .build()

    @Before
    fun setUp() {
        val credsStream = FileInputStream("../../gcp-service-account.json")
        credsProvider = FixedCredentialsProvider.create(GoogleCredentials.fromStream(credsStream))
    }

    @Test
    fun `it should aggregate all names into value`() {
        val pipeline = basicPipeline {
            basicConfig()
            defaultOpts = Opts(consumers = 3)
            aggregate("input", aggregate = { s, m ->
                s["names"] = (s["names"] ?: "") + m["name"] + ","; m
            },)
        }

        val allNames = listOf("Vasya", "Petya", "Poheru", "Malina")
        allNames.forEach { name ->
            pipeline.send("input", mutableMapOf("name" to name))
        }

        await().atMost(20, SECONDS).until(
            { pipeline.repo("input")["input"] },
            { it?.get("names")?.split(",")?.containsAll(allNames) ?: false },
        )

        pipeline.stop()
    }

    @Test
    fun `it should calculate sum of all numbers`() {
        val pipeline: io.github.smecsia.poreia.core.Pipeline<Int, MutableMap<String, Int>> = startPipeline {
            stateInit = { mutableMapOf() }
            defaultOpts = Opts(consumers = 3)
            basicConfig()

            aggregate("sum", aggregate = { s, m -> s["sum"] = (s["sum"] ?: 0) + m; m })
        }
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).forEach { number ->
            pipeline.send("sum", number)
        }
        await().atMost(30, SECONDS).until(
            { pipeline.repo("sum")["sum"] },
            { it?.get("sum") == 55 },
        )

        pipeline.stop()
    }

    private fun <M, S> io.github.smecsia.poreia.core.Pipeline<M, S>.basicConfig() {
        bcBuilder = PubSubBroadcastBuilder(
            projectId,
            credentialsProvider = credsProvider,
            channelProvider = channelProvider,
        )
        queueBuilder = PubSubQueueBuilder(
            projectId,
            credentialsProvider = credsProvider,
            channelProvider = channelProvider,
        )
        repoBuilder = singletonRepoBuilder(inMemoryRepo(stateInit))
    }
}
