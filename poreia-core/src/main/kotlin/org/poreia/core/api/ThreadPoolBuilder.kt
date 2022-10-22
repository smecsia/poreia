package org.poreia.core.api

interface ThreadPoolBuilder {
    fun build(size: Int): ThreadPool
}
