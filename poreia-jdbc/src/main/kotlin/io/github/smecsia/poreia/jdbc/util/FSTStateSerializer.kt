package io.github.smecsia.poreia.jdbc.util

import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.nustaq.serialization.FSTConfiguration

@Suppress("UNCHECKED_CAST")
class FSTStateSerializer<S> : ToBytesSerializer<S> {
    override fun serialize(obj: S): ByteArray {
        return serializer.asByteArray(obj)
    }

    override fun deserialize(obj: ByteArray): S {
        return serializer.asObject(obj) as S
    }

    companion object {
        private val serializer = FSTConfiguration.createDefaultConfiguration()
    }
}
