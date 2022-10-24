package io.github.smecsia.poreia.ext.activemq

import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

abstract class AbstractActivemqBuilder<M>(
    protected val factory: ConnectionFactory,
    protected val serializer: ToBytesSerializer<M>,
)
