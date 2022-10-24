package io.github.smecsia.poreia.core.api.serialize

interface Serializer<From, To> {
    fun serialize(obj: From): To
    fun deserialize(obj: To): From
}
