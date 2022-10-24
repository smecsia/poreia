package io.github.smecsia.poreia.core.api.serialize

interface StateSerializer<S, To> : Serializer<S, To> {
    override fun serialize(obj: S): To
    override fun deserialize(obj: To): S
}
