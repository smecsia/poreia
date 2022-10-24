package io.github.smecsia.poreia.core.error

class InvalidLockOwnerException @JvmOverloads constructor(msg: String, e: Exception? = null) :
    PoreiaRuntimeException(msg, e)
