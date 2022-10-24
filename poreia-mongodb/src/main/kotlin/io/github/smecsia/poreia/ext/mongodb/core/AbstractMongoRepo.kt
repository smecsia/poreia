package io.github.smecsia.poreia.ext.mongodb.core

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import io.github.smecsia.poreia.ext.mongodb.DefaultToBsonSerializer
import io.github.smecsia.poreia.ext.mongodb.ToBsonSerializer
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry

abstract class AbstractMongoRepo<T : Any>(
    protected val mongo: MongoClient,
    protected val dbName: String,
    protected val collection: String,
    private val serializer: ToBsonSerializer<T>? = DefaultToBsonSerializer(),
    private val codecRegistry: CodecRegistry? = null,
) {
    protected fun collection(): MongoCollection<Document> {
        return db().getCollection(collection).let {
            if (codecRegistry != null) {
                it.withCodecRegistry(codecRegistry)
            } else {
                it
            }
        }
    }

    protected fun valueToDocument(value: T): Document {
        return serializer!!.serialize(value)
    }

    protected fun documentToValue(doc: Document?): T? {
        return doc?.let { serializer?.deserialize(it) }
    }

    protected fun collectionDoc(): MongoCollection<Document> =
        db().getCollection(collection, Document::class.java)

    protected fun db(): MongoDatabase {
        return mongo.getDatabase(dbName)
    }
}
