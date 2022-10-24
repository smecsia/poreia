package io.github.smecsia.poreia.core.error

open class PoreiaRuntimeException(msg: String = "", e: Exception? = null) : RuntimeException(msg, e)
