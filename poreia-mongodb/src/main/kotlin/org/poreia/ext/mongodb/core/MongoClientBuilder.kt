package org.poreia.ext.mongodb.core

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import org.litote.kmongo.KMongo.createClient

object MongoClientBuilder {

    fun simpleMongoClient(cs: String): MongoClient {
        val clientSettings = MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(cs))
            .build()

        return createClient(clientSettings)
    }
}
