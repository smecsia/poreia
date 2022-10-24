package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.ThreadPool
import io.github.smecsia.poreia.core.api.ThreadPoolBuilder
import java.util.concurrent.Executors.newFixedThreadPool

class BasicThreadPoolBuilder : ThreadPoolBuilder {
    override fun build(size: Int): ThreadPool {
        val executor = newFixedThreadPool(size)
        return object : ThreadPool {
            override fun shutdownNow() {
                executor.shutdownNow()
            }

            override fun submit(task: () -> Unit) {
                executor.submit(task)
            }

            override fun isShutdown(): Boolean {
                return executor.isShutdown
            }
        }
    }
}
