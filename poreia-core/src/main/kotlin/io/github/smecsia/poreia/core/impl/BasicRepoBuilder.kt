package io.github.smecsia.poreia.core.impl

import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer

class BasicRepoBuilder<S> : RepoBuilder<S> {
    override fun build(
        name: String,
        opts: Opts,
        stateInit: StateInitializer<S>?,
        stateClass: Class<S>?,
    ): Repository<S> {
        return ConcurrentHashMapRepository(stateInit, locker = ConcurrentHashMapLocker(opts))
    }
}
