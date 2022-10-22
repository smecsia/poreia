package org.poreia.ext.activemq.util

import org.apache.activemq.ActiveMQConnectionFactory
import org.junit.rules.ExternalResource
import javax.jms.ConnectionFactory

class ActivemqRule(private val container: ActivemqContainer = ActivemqContainer()) : ExternalResource() {

    override fun before() {
        container.start()
    }

    override fun after() {
        container.stop()
    }

    val factory: ConnectionFactory by lazy {
        ActiveMQConnectionFactory("tcp://$host:$port")
    }

    val port by lazy {
        container.port
    }

    val host by lazy {
        container.host
    }
}
