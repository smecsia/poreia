package io.github.smecsia.poreia.ext.activemq

import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import org.apache.activemq.ActiveMQSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.jms.BytesMessage
import javax.jms.Connection
import javax.jms.ConnectionFactory
import javax.jms.DeliveryMode.PERSISTENT
import javax.jms.Destination
import javax.jms.JMSException
import javax.jms.MessageProducer
import javax.jms.Session

abstract class AbstractActivemqConsumer<M>(
    private val queueName: String,
    private val factory: ConnectionFactory,
    private val serializer: ToBytesSerializer<M>,
    private val opts: Opts,
) {
    protected abstract fun initDestination(session: Session, name: String): Destination

    @Throws(JMSException::class)
    protected fun produce(event: M) {
        if (session is ActiveMQSession && (session as ActiveMQSession).isClosed) {
            initProducer()
        }
        val message: BytesMessage = try {
            session.createBytesMessage()
        } catch (e: javax.jms.IllegalStateException) {
            initProducer()
            session.createBytesMessage()
        }
        message.writeBytes(serializer.serialize(event))
        producer.send(message)
    }

    @Throws(JMSException::class)
    protected fun newConsumer(): ActivemqConsumer<M> {
        val session = newSession()
        return ActivemqConsumer(session.createConsumer(initDestination(session, queueName)), serializer, queueName, opts)
    }

    @Throws(JMSException::class)
    protected fun newSession(): Session {
        LOGGER.debug("Creating consumer connection to JMS...")
        val cConnection = factory.createConnection() as Connection
        cConnection.start()
        return cConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    }

    protected val LOGGER: Logger = LoggerFactory.getLogger(javaClass)

    @Volatile
    private lateinit var producer: MessageProducer

    @Volatile
    private lateinit var session: Session

    @Volatile
    private lateinit var destination: Destination

    init {
        initProducer()
    }

    private fun initProducer() {
        LOGGER.debug("Creating producer connection to JMS...")
        try {
            destination = initDestination(newSession().also { session = it }, queueName)
            producer = session.createProducer(destination)
            producer.deliveryMode = PERSISTENT
            LOGGER.debug("Destination $destination initialized for $factory")
        } catch (e: JMSException) {
            throw RuntimeException(e)
        }
    }
}
