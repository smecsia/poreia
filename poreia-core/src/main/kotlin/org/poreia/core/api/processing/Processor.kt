package org.poreia.core.api.processing

import org.poreia.core.api.queue.QueueConsumer

interface Processor<M> {
    val name: String

    fun run(consumer: QueueConsumer<M>, consumerName: String = "0")

    fun broadcast(vararg targets: String): Processor<M>

    fun output(vararg targets: String): Processor<M>
}
