package org.poreia.ext.gcloud

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.queue.QueueBuilder
import org.poreia.core.api.serialize.ToBytesSerializer
import org.poreia.core.impl.BasicSerializer

class PubSubQueueBuilder<M>(
    private val projectId: String,
    private val channelProvider: TransportChannelProvider? = null,
    private val credentialsProvider: CredentialsProvider? = null,
    private val serializer: ToBytesSerializer<M> = BasicSerializer(),
) : QueueBuilder<M, Queue<M>> {
    
    override fun build(name: String, pipeline: Pipeline<M, *>, opts: Opts): PubSubQueue<M> {
        return PubSubQueue(pipeline, projectId, channelProvider, credentialsProvider, name, serializer, opts)
    }
}
