package org.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.BroadcastBuilder
import org.poreia.core.api.queue.Broadcaster
import org.poreia.core.api.serialize.ToBytesSerializer
import org.poreia.core.impl.BasicSerializer

class PubSubBroadcastBuilder<M>(
    private val projectId: String,
    private val channelProvider: TransportChannelProvider? = null,
    private val credentialsProvider: CredentialsProvider? = null,
    private val serializer: ToBytesSerializer<M> = BasicSerializer(),
) : BroadcastBuilder<M> {

    override fun build(target: String, pipeline: Pipeline<M, *>, opts: Opts): Broadcaster<M> {
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
