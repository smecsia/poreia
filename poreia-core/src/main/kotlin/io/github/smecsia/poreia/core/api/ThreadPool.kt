package io.github.smecsia.poreia.core.api

interface ThreadPool {
    fun shutdownNow()

    fun submit(task: () -> Unit)

    fun isShutdown(): Boolean
}
