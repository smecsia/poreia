package io.github.smecsia.poreia.core.error

class LockWaitTimeoutException @JvmOverloads constructor(msg: String, e: Exception? = null) :
    PoreiaRuntimeException(msg, e)
