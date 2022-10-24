package io.github.smecsia.poreia.ext.serializer.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.slf4j.LoggerFactory

class JacksonToBytesSerializer<M : Any> : ToBytesSerializer<M> {

    override fun serialize(obj: M): ByteArray {
        return objectMapper.writeValueAsBytes(
            JacksonObjectWrapper(
                type = obj.javaClass.name,
                serializedObject = objectMapper.writeValueAsString(obj),
            ),
        )
    }

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(obj: ByteArray): M {
        val wrapper = objectMapper.readValue(obj, JacksonObjectWrapper::class.java)
        val resClass = Class.forName(wrapper.type)
        LOG.trace("Class.forName(${wrapper.type})=$resClass...")
        return objectMapper.readValue(wrapper.serializedObject, resClass) as M
    }

    companion object {
        private val objectMapper = ObjectMapper()
            .registerModule(KotlinModule.Builder().build())
        private val LOG = LoggerFactory.getLogger(JacksonToBytesSerializer::class.java)
    }
}
