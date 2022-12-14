package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.api.Opts

/**
 * Builder for [Repository] instances
 */
interface RepoBuilder<S> {
    fun build(
        name: String,
        opts: Opts = Opts(),
        stateInit: StateInitializer<S>? = null,
        stateClass: Class<S>? = null,
    ): Repository<S>
}
