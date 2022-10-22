package org.poreia.ext.activemq

import org.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory

abstract class AbstractActivemqBuilder<M>(
    protected val factory: ConnectionFactory,
    protected val serializer: ToBytesSerializer<M>
)
