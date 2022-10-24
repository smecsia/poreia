package io.github.smecsia.poreia.ext.activemq.util

import org.testcontainers.containers.GenericContainer

/**
 * This [ActivemqContainer] is based on the ActiveMQ image from
 * [DockerHub](https://hub.docker.com/r/rmohr/activemq/)
 */
class ActivemqContainer @JvmOverloads constructor(image: String = DEFAULT_IMAGE_AND_TAG) :
    GenericContainer<ActivemqContainer?>(image) {
    /**
     * Returns the actual public port of the internal Activemq port ({@value ACTIVEMQ_PORT}).
     *
     * @return the public port of this container
     * @see .getMappedPort
     */
    val port: Int
        get() = getMappedPort(ACTIVEMQ_PORT)

    /**
     * Returns the actual public port of the internal Activemq HTTP port ({@value ACTIVEMQ_HTTP_PORT}).
     *
     * @return the public port of this container
     * @see .getMappedPort
     */
    val httpPort: Int
        get() = getMappedPort(ACTIVEMQ_HTTP_PORT)

    companion object {
        /**
         * This is the internal port on which Activemq is running inside the container.
         *
         *
         * You can use this constant in case you want to map an explicit public port to it
         * instead of the default random port. This can be done using methods like
         * [.setPortBindings].
         */
        const val ACTIVEMQ_PORT = 61616
        const val ACTIVEMQ_HTTP_PORT = 8161
        const val DEFAULT_IMAGE_AND_TAG = "rmohr/activemq:5.15.9"
    }

    /**
     * Creates a new [ActivemqContainer] with the {@value DEFAULT_IMAGE_AND_TAG} image.
     */
    init {
        addExposedPort(ACTIVEMQ_PORT)
        addExposedPort(ACTIVEMQ_HTTP_PORT)
    }
}
