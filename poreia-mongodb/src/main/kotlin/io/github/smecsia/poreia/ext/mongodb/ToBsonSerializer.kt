package io.github.smecsia.poreia.ext.mongodb

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.github.smecsia.poreia.core.api.serialize.Serializer
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.bson.Document
import org.bson.types.Binary

interface ToBsonSerializer<T> : Serializer<T, Document>

@Suppress("UNCHECKED_CAST")
class DefaultToBsonSerializer<T : Any> : ToBsonSerializer<T> {
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())

    override fun serialize(obj: T): Document {
        return Document(
            mapOf(
                "value" to objectMapper.writeValueAsString(obj),
                "type" to obj.javaClass.name,
            ),
        )
    }

    override fun deserialize(obj: Document): T {
        val type = obj["type"] as String
        val usedClass = Class.forName(type)
        return objectMapper.readValue(obj["value"] as String, usedClass) as T
    }
}

class ToBytesToBsonSerializer<T : Any>(private val serializer: ToBytesSerializer<T>) : ToBsonSerializer<T> {
    override fun serialize(obj: T): Document {
        return Document(
            mapOf(
                "value" to serializer.serialize(obj),
            ),
        )
    }

    override fun deserialize(obj: Document): T {
        val binary = obj["value"] as Binary
        return serializer.deserialize(binary.data)
    }
}
