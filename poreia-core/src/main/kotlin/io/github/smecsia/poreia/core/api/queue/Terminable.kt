package io.github.smecsia.poreia.core.api.queue

/**
 * Represents the instance that can be terminated when something needs to be interrupted
 */
interface Terminable {
    fun terminate() = Unit
}
