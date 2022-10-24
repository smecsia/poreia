package io.github.smecsia.poreia.ext.rabbitmq.util

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.junit.rules.ExternalResource
import org.testcontainers.containers.RabbitMQContainer

class RabbitmqRule(private val container: RabbitMQContainer = RabbitMQContainer("rabbitmq:3.7.25-management-alpine")) : ExternalResource() {

    override fun before() {
        container.start()
    }

    override fun after() {
        container.stop()
    }

    val connection: Connection by lazy {
        val factory = ConnectionFactory()
        factory.setUri(container.amqpUrl)
        factory.username = container.adminUsername
        factory.password = container.adminPassword
        factory.newConnection()
    }
}
