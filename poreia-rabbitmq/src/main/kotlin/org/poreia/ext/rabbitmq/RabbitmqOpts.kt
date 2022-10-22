package org.poreia.ext.rabbitmq

data class RabbitmqOpts(
    val durable: Boolean = true,
    val exclusive: Boolean = false,
    val autoDelete: Boolean = false,
    val args: Map<String, Any> = emptyMap()
)
