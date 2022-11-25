package io.github.smecsia.poreia.core.api

/**
 * Base interface for thread pools
 */
interface ThreadPool {
    fun shutdownNow()

    fun submit(task: () -> Unit)

    fun isShutdown(): Boolean
}
