package io.github.smecsia.poreia.core.api

import java.io.Serializable
import java.lang.System.currentTimeMillis

interface ClusterAware {
    enum class Role { PRIMARY, REPLICA }
    data class Heartbeat(val lastUpdated: Long, val nodeId: String) : Serializable

    fun start()
    fun restart()
    fun suspend()
    fun terminate(): Boolean

    val role: Role

    val name: String

    fun newHeartbeat() = Heartbeat(currentTimeMillis(), name)
}
