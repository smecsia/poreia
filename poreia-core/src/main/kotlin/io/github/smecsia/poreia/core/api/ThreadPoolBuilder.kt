package io.github.smecsia.poreia.core.api

interface ThreadPoolBuilder {
    fun build(size: Int): ThreadPool
}
