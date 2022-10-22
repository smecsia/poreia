package org.poreia.core.api.processing

import org.poreia.core.api.Opts

interface RepoBuilder<S> {
    fun build(
        name: String,
        opts: Opts = Opts(),
        stateInit: StateInitializer<S>? = null,
        stateClass: Class<S>? = null
    ): Repository<S>
}
