package org.poreia.ext.gcloud

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannelBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.Rule
import org.junit.Test
import org.poreia.ext.gcloud.util.PubSubRule
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName
import java.io.IOException


class PubSubTest {
    @Rule
    @JvmField
    var pubsub = PubSubRule()

    val PROJECT_ID = "project"

    @Throws(IOException::class)
    private fun createTopic(
        topicId: String,
        channelProvider: TransportChannelProvider,
        credentialsProvider: NoCredentialsProvider
    ) {
        val topicAdminSettings = TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build()
        TopicAdminClient.create(topicAdminSettings).use { topicAdminClient ->
            val topicName = TopicName.of(PROJECT_ID, topicId)
            topicAdminClient.createTopic(topicName)
        }
    }


    @Throws(IOException::class)
    private fun createSubscription(
        subscriptionId: String,
        topicId: String,
        channelProvider: TransportChannelProvider,
        credentialsProvider: NoCredentialsProvider
    ) {
        val subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build()
        val subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)
        val subscriptionName = SubscriptionName.of(PROJECT_ID, subscriptionId)
        subscriptionAdminClient.createSubscription(
            subscriptionName,
            TopicName.of(PROJECT_ID, topicId),
            PushConfig.getDefaultInstance(),
            10
        )
    }


    @Test
    fun testSimple() {
        val channel = ManagedChannelBuilder.forTarget(pubsub.endpoint).usePlaintext().build()
        try {
            val channelProvider: TransportChannelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
            val credentialsProvider = NoCredentialsProvider.create()
            val topicId = "my-topic-id"
            createTopic(topicId, channelProvider, credentialsProvider)
            val subscriptionId = "my-subscription-id"
            createSubscription(subscriptionId, topicId, channelProvider, credentialsProvider)
            val publisher: Publisher = Publisher.newBuilder(TopicName.of(PROJECT_ID, topicId))
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
            val message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("test message")).build()
            publisher.publish(message)
            val subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
            GrpcSubscriberStub.create(subscriberStubSettings).use { subscriber ->
                val pullRequest = PullRequest.newBuilder()
                    .setMaxMessages(1)
                    .setSubscription(ProjectSubscriptionName.format(PROJECT_ID, subscriptionId))
                    .build()
                val pullResponse = subscriber.pullCallable().call(pullRequest)
                assertThat(pullResponse.receivedMessagesList).hasSize(1)
                assertThat(
                    pullResponse.getReceivedMessages(0).message.data.toStringUtf8()
                ).isEqualTo("test message")
            }
        } finally {
            channel.shutdown()
        }
    }
}