package org.poreia.ext.serializer.jackson

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class JacksonObjectWrapper @JsonCreator constructor(
    @JsonProperty("type")
    val type: String,
    @JsonProperty("serializedObject")
    val serializedObject: String
)
