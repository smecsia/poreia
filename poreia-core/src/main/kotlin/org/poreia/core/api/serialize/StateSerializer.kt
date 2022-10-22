package org.poreia.core.api.serialize

import java.io.Serializable

interface StateSerializer<S, To> : Serializer<S, To> {
    override fun serialize(obj: S): To
    override fun deserialize(obj: To): S
}
