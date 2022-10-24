package io.github.smecsia.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.AlreadyExistsException
import com.google.api.gax.rpc.ClientSettings
import com.google.api.gax.rpc.NotFoundException
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.Topic
import com.google.pubsub.v1.TopicName
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Terminable
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

abstract class AbstractPubSubQueue<M>(
    private val projectId: String,
    private val channelProvider: TransportChannelProvider? = null,
    private val credentialsProvider: CredentialsProvider? = null,
    protected val topicId: String,
    private val serializer: ToBytesSerializer<M>,
    private val opts: Opts = Opts(),
) : Terminable {
    private val topic: Topic? by lazy {
        createTopicIfNotExists(TopicName.of(projectId, topicId))
    }
    private val publisher: Publisher by lazy {
        initPublisher()
    }
    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun terminate() {
        publisher.shutdown()
        publisher.awaitTermination(1, TimeUnit.SECONDS)
    }

    private fun createTopicIfNotExists(topicName: TopicName): Topic? {
        val topicAdminSettings = TopicAdminSettings.newBuilder()
            .configureClient()
            .build()
        return TopicAdminClient.create(topicAdminSettings).use { topicAdminClient ->
            try {
                topicAdminClient.getTopic(topicName)
            } catch (e: NotFoundException) {
                topicAdminClient.createTopic(topicName)
            }
        }
    }

    private fun createSubscriptionIfNotExists(subscriptionId: String) {
        val subscriptionAdmin = SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder().configureClient().build(),
        )
        val subscriptionName = SubscriptionName.of(projectId, subscriptionId)
        try {
            subscriptionAdmin.use { client ->
                try {
                    client.getSubscription(subscriptionName)
                } catch (e: NotFoundException) {
                    client.createSubscription(
                        subscriptionName,
                        topic?.name,
                        PushConfig.getDefaultInstance(),
                        opts.ackDeadlineSec,
                    )
                }
            }
        } catch (e: AlreadyExistsException) {
            logger.debug("Subscription $subscriptionId already exists, ignoring...", e)
        }
    }

    protected fun publishMessage(message: M): String {
        val msg = publisher.publish(
            PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(serializer.serialize(message)))
                .build(),
        )
        logger.debug("Message $message sent to ${topic?.name}")
        return msg.get()
    }

    protected fun initConsumer(subscriptionId: String): PubSubConsumer<M> {
        createSubscriptionIfNotExists(subscriptionId)
        val builder = SubscriberStubSettings.newBuilder().apply {
            val queue = this@AbstractPubSubQueue
            if (queue.channelProvider != null) {
                transportChannelProvider = queue.channelProvider
            }
            if (queue.credentialsProvider != null) {
                credentialsProvider = queue.credentialsProvider
            }
        }
        val subscriberStubSettings = builder.build()

        return PubSubConsumer(
            projectId,
            subscriptionId,
            GrpcSubscriberStub.create(subscriberStubSettings),
            serializer,
            topicId,
            opts,
        )
    }

    private fun <S : ClientSettings<S>, B : ClientSettings.Builder<S, B>> ClientSettings.Builder<S, B>.configureClient():
        ClientSettings.Builder<S, B> {
        val queue = this@AbstractPubSubQueue
        if (queue.channelProvider != null) {
            this.transportChannelProvider = queue.channelProvider
        }
        if (queue.credentialsProvider != null) {
            this.credentialsProvider = queue.credentialsProvider
        }
        return this
    }

    private fun initPublisher(): Publisher {
        val builder = Publisher.newBuilder(topic?.name)
        if (channelProvider != null) {
            builder.setChannelProvider(channelProvider)
        }
        if (credentialsProvider != null) {
            builder.setCredentialsProvider(credentialsProvider)
        }
        return builder.build()
    }
}
