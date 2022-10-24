package io.github.smecsia.poreia.ext.gcloud

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PullRequest
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.QueueConsumer
import io.github.smecsia.poreia.core.api.queue.QueueConsumerCallback
import io.github.smecsia.poreia.core.api.queue.ackOnError
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class PubSubConsumer<M>(
    private val projectId: String,
    private val subscriptionId: String,
    private val subscriberStub: SubscriberStub,
    private val serializer: ToBytesSerializer<M>,
    override val queueName: String,
    private val opts: Opts,
) : QueueConsumer<M> {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun terminate() {
        subscriberStub.shutdownNow()
        subscriberStub.awaitTermination(1, TimeUnit.SECONDS)
    }

    override fun consume(callback: QueueConsumerCallback<M>) {
        val subscription = ProjectSubscriptionName.format(projectId, subscriptionId)
        subscriberStub.let { subscriber ->
            val pullRequest = PullRequest.newBuilder().setMaxMessages(1)
                .setSubscription(subscription).build()
            val pullResponse = try {
                subscriber.pullCallable().call(pullRequest)
            } catch (e: Exception) {
                if (subscriberStub.isShutdown) {
                    logger.debug("Failed to pull message from $queueName due to thread pool shutdown", e)
                    return
                }
                logger.error("Failed to pull message from $queueName by subscription $subscriptionId", e)
                throw e
            }
            if (pullResponse.receivedMessagesCount != 1) {
                return // no messages were consumed, releasing call
            }
            val receivedMessage = pullResponse.getReceivedMessages(0)
            val deserializedMsg = serializer.deserialize(receivedMessage.message.data.toByteArray())
            callback.ackOnError(deserializedMsg, ackOnError = opts.ackOnError) {
                subscriber.acknowledgeCallable()
                    .call(
                        AcknowledgeRequest.newBuilder()
                            .setSubscription(subscription)
                            .addAckIds(receivedMessage.ackId)
                            .build(),
                    )
            }
        }
    }
}
