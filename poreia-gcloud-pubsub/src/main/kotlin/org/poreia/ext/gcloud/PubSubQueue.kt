package org.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.serialize.ToBytesSerializer
import java.util.concurrent.TimeUnit


class PubSubQueue<M> @JvmOverloads constructor(
    pipeline: Pipeline<M, *>,
    projectId: String,
    channelProvider: TransportChannelProvider? = null,
    credentialsProvider: CredentialsProvider? = null ,
    topicId: String,
    serializer: ToBytesSerializer<M>,
    opts: Opts = Opts(),
) : AbstractPubSubQueue<M>(
    projectId = projectId,
    channelProvider = channelProvider,
    credentialsProvider = credentialsProvider,
    topicId = "q-${pipeline.name}-$topicId",
    serializer = serializer,
    opts = opts
), Queue<M> {

    override fun add(message: M) {
        publishMessage(message)
    }

    override fun buildConsumer(name: String): PubSubConsumer<M> = initConsumer(topicId)

    override fun isEmpty(): Boolean = throw NotImplementedError("'isEmpty' not implemented for PubSubQueue")

    override fun count(): Int = throw NotImplementedError("'count' not implemented for PubSubQueue")
}
