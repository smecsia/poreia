package io.github.smecsia.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.queue.Queue
import io.github.smecsia.poreia.core.api.queue.QueueBuilder
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import io.github.smecsia.poreia.core.impl.BasicSerializer

class PubSubQueueBuilder<M>(
    private val projectId: String,
    private val channelProvider: TransportChannelProvider? = null,
    private val credentialsProvider: CredentialsProvider? = null,
    private val serializer: ToBytesSerializer<M> = BasicSerializer(),
) : QueueBuilder<M, Queue<M>> {

    override fun build(name: String, pipeline: io.github.smecsia.poreia.core.Pipeline<M, *>, opts: Opts): PubSubQueue<M> {
        return PubSubQueue(pipeline, projectId, channelProvider, credentialsProvider, name, serializer, opts)
    }
}
