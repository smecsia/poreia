package io.github.smecsia.poreia.core.util

import java.lang.Thread.currentThread
import java.lang.management.ManagementFactory.getRuntimeMXBean

/**
 * Utility class to manipulate and query thread id
 */
object ThreadUtil {
    private val THREAD_LOCAL = ThreadLocal<Long>()

    /**
     * Get the thread id
     *
     * @return the thread id
     */
    @JvmStatic
    fun threadId(): String {
        val threadId = THREAD_LOCAL.get()
        return if (threadId != null) {
            getRuntimeMXBean().name + "-" + threadId
        } else {
            getRuntimeMXBean().name + "-" + currentThread().id
        }
    }
}
