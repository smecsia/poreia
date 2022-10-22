package org.poreia.core.api.processing

abstract class ProcessingException(
    override val message: String,
    override val cause: Exception
) : RuntimeException(message, cause)
