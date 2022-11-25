package io.github.smecsia.poreia.core.api

/**
 * Builder for [ThreadPool] instances
 */
interface ThreadPoolBuilder {
    fun build(size: Int): ThreadPool
}
