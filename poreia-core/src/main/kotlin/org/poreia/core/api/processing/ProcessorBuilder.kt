package org.poreia.core.api.processing

import org.poreia.core.Pipeline
import org.poreia.core.api.Opts
import org.poreia.core.api.ProcessingStrategy

interface ProcessorBuilder<M> {
    fun build(
        pipeline: Pipeline<M, *>,
        name: String,
        strategy: ProcessingStrategy<M>,
        filter: Filter<M>? = null,
        opts: Opts = Opts()
    ): Processor<M>
}
