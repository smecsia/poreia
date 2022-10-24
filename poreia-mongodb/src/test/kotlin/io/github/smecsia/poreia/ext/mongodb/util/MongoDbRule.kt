package io.github.smecsia.poreia.ext.mongodb.util

import io.github.smecsia.poreia.ext.mongodb.core.MongoClientBuilder.simpleMongoClient
import org.junit.rules.ExternalResource

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
