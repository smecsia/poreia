package io.github.smecsia.poreia.core

import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.impl.BasicRepoBuilder

typealias Message<T> = Map<String, T>
typealias State<T> = MutableMap<String, T>

typealias SimpleMessage = Message<Any>
typealias SimpleState = State<Any>
typealias SimplePipeline = io.github.smecsia.poreia.core.Pipeline<SimpleMessage, SimpleState>
typealias BasicPipeline = io.github.smecsia.poreia.core.Pipeline<Message<String>, State<String>>

fun basicPipeline(name: String = "pipeline", definition: BasicPipeline.() -> Unit): BasicPipeline =
    startPipeline(name) {
        stateInit = { mutableMapOf() }
        definition(this)
    }

@Suppress("UNCHECKED_CAST")
fun simplePipeline(
    name: String = "pipeline",
    definition: SimplePipeline.() -> Unit,
): SimplePipeline =
    startPipeline(name) {
        stateInit = { mutableMapOf() }
        stateClass = LinkedHashMap<String, Any>()::class.java as Class<SimpleState>
        definition(this)
    }

fun <S> singletonRepoBuilder(repo: Repository<S>): RepoBuilder<S> {
    return object : RepoBuilder<S> {
        override fun build(
            name: String,
            opts: Opts,
            stateInit: StateInitializer<S>?,
            stateClass: Class<S>?,
        ): Repository<S> {
            return repo
        }
    }
}

fun <S> inMemoryRepo(stateInitializer: StateInitializer<S>? = null): Repository<S> {
    return BasicRepoBuilder<S>().build("", stateInit = stateInitializer)
}
