package io.github.smecsia.poreia.ext.mongodb.util

import org.testcontainers.containers.GenericContainer

/**
 * This [MongoDbContainer] is based on the official MongoDb (`mongo`) image from
 * [DockerHub](https://hub.docker.com/r/_/mongo/). If you need to use a custom MongoDB
 * image, you can provide the full image name as well.
 *
 * @author Stefan Ludwig
 */
class MongoDbContainer @JvmOverloads constructor(image: String = DEFAULT_IMAGE_AND_TAG) :
    GenericContainer<MongoDbContainer?>(image) {
    /**
     * Returns the actual public port of the internal MongoDB port ({@value MONGODB_PORT}).
     *
     * @return the public port of this container
     * @see .getMappedPort
     */
    val port: Int
        get() = getMappedPort(MONGODB_PORT)

    companion object {
        /**
         * This is the internal port on which MongoDB is running inside the container.
         *
         *
         * You can use this constant in case you want to map an explicit public port to it
         * instead of the default random port. This can be done using methods like
         * [.setPortBindings].
         */
        const val MONGODB_PORT = 27017
        const val DEFAULT_IMAGE_AND_TAG = "mongo:4.2"
    }
    /**
     * Creates a new [MongoDbContainer] with the given `'image'`.
     *
     * @param image the image (e.g. {@value DEFAULT_IMAGE_AND_TAG}) to use
     */
    /**
     * Creates a new [MongoDbContainer] with the {@value DEFAULT_IMAGE_AND_TAG} image.
     */
    init {
        addExposedPort(MONGODB_PORT)
    }
}
