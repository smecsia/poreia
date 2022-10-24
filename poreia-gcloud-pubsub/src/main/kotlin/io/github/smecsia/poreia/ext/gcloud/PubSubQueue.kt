package io.github.smecsia.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer

class PubSubQueue<M> @JvmOverloads constructor(
    pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>,
    projectId: String,
    channelProvider: TransportChannelProvider? = null,
    credentialsProvider: CredentialsProvider? = null,
    topicId: String,
    serializer: ToBytesSerializer<M>,
    opts: Opts = Opts(),
) : AbstractPubSubQueue<M>(
    projectId = projectId,
    channelProvider = channelProvider,
    credentialsProvider = credentialsProvider,
    topicId = "q-${pipeline.name}-$topicId",
    serializer = serializer,
    opts = opts,
),
    Queue<M> {

    override fun add(message: M) {
        publishMessage(message)
    }

    override fun buildConsumer(name: String): PubSubConsumer<M> = initConsumer(topicId)

    override fun isEmpty(): Boolean = throw NotImplementedError("'isEmpty' not implemented for PubSubQueue")

    override fun count(): Int = throw NotImplementedError("'count' not implemented for PubSubQueue")
}
