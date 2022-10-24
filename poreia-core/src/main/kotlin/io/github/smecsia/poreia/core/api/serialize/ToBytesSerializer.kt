package io.github.smecsia.poreia.core.api.serialize

interface ToBytesSerializer<M> : Serializer<M, ByteArray> {
    override fun serialize(obj: M): ByteArray
    override fun deserialize(obj: ByteArray): M
}
