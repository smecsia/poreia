package org.poreia.core.impl

import org.poreia.core.api.Opts
import org.poreia.core.api.processing.RepoBuilder
import org.poreia.core.api.processing.Repository
import org.poreia.core.api.processing.StateInitializer

class BasicRepoBuilder<S> : RepoBuilder<S> {
    override fun build(
        name: String,
        opts: Opts,
        stateInit: StateInitializer<S>?,
        stateClass: Class<S>?
    ): Repository<S> {
        return ConcurrentHashMapRepository(stateInit, locker = ConcurrentHashMapLocker(opts))
    }
}
