package org.poreia.ext.mongodb.util

import org.junit.rules.ExternalResource
import org.poreia.ext.mongodb.core.MongoClientBuilder.simpleMongoClient

class MongoDbRule(private val container: MongoDbContainer = MongoDbContainer()) : ExternalResource() {

    override fun before() {
        container.start()
    }

    override fun after() {
        client.close()
        container.stop()
    }

    val port by lazy {
        container.port
    }

    val host by lazy {
        container.host
    }

    val client by lazy {
        simpleMongoClient("mongodb://$host:$port")
    }
}
