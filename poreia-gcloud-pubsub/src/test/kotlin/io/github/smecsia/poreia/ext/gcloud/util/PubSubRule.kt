package io.github.smecsia.poreia.ext.gcloud.util

import org.junit.rules.ExternalResource
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName

class PubSubRule(
    private val container: PubSubEmulatorContainer = PubSubEmulatorContainer(
        DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"),
    ),
) : ExternalResource() {

    override fun before() {
        container.start()
    }

    override fun after() {
        container.stop()
    }

    val endpoint by lazy {
        container.emulatorEndpoint
    }
}
