package io.github.smecsia.poreia.core.api.processing

import io.github.smecsia.poreia.core.Pipeline
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.ProcessingStrategy

/**
 * Builder for [Processor] instances
 */
interface ProcessorBuilder<M> {
    fun build(
        pipeline: Pipeline<M, *>,
        name: String,
        strategy: ProcessingStrategy<M>,
        filter: Filter<M>? = null,
        opts: Opts = Opts(),
    ): Processor<M>
}
