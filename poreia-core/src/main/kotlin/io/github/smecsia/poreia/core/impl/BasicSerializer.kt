package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

class BasicSerializer<M> : ToBytesSerializer<M> {
    override fun serialize(obj: M): ByteArray {
        val output = ByteArrayOutputStream()
        ObjectOutputStream(output).use {
            it.writeObject(obj)
        }
        return output.toByteArray()
    }

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(obj: ByteArray): M {
        ObjectInputStream(ByteArrayInputStream(obj)).use {
            return it.readObject() as M
        }
    }
}
