package org.poreia.core.impl

import org.poreia.core.api.ThreadPool
import org.poreia.core.api.ThreadPoolBuilder
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
