package org.poreia.ext.activemq

import org.poreia.core.api.Opts
import org.poreia.core.api.queue.Queue
import org.poreia.core.api.serialize.ToBytesSerializer
import javax.jms.ConnectionFactory
import javax.jms.QueueBrowser
import javax.jms.Session

class ActivemqQueue<M> @JvmOverloads constructor(
    destName: String,
    factory: ConnectionFactory,
    serializer: ToBytesSerializer<M>,
    opts: Opts = Opts()
) : AbstractActivemqConsumer<M>(destName, factory, serializer, opts),
    Queue<M> {

    private lateinit var browser: QueueBrowser

    override fun add(message: M) {
        produce(message)
    }

    override fun buildConsumer(name: String): ActivemqConsumer<M> {
        return super.newConsumer()
    }

    override fun initDestination(session: Session, name: String): javax.jms.Queue {
        val queue = session.createQueue(name)
        browser = session.createBrowser(queue)
        return queue
    }

    override fun isEmpty(): Boolean {
        return !browser.enumeration.hasMoreElements()
    }

    override fun count(): Int {
        return browser.enumeration.toList().size
    }
}
