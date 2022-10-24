package io.github.smecsia.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.BroadcastBuilder
import io.github.smecsia.poreia.core.api.queue.Broadcaster
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import io.github.smecsia.poreia.core.impl.BasicSerializer

class PubSubBroadcastBuilder<M>(
    private val projectId: String,
    private val channelProvider: TransportChannelProvider? = null,
    private val credentialsProvider: CredentialsProvider? = null,
    private val serializer: ToBytesSerializer<M> = BasicSerializer(),
) : BroadcastBuilder<M> {

    override fun build(target: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): Broadcaster<M> {
        return PubSubBroadcaster(
            projectId = projectId,
            channelProvider = channelProvider,
            credentialsProvider = credentialsProvider,
            topicId = target,
            pipeline = pipeline,
            serializer = serializer,
            opts = opts,
        )
    }
}
