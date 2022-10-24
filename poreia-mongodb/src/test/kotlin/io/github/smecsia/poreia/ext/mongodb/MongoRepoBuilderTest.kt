package io.github.smecsia.poreia.ext.mongodb

import io.github.smecsia.poreia.core.SimpleState
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.junit.Rule
import org.junit.Test

class MongoRepoBuilderTest {
    @Rule
    @JvmField
    var mongodb: MongoDbRule = MongoDbRule()

    @Test
    fun `it should figure state class from initializer`() {
        val builder = MongoRepoBuilder<SimpleState>(mongodb.client, "test")

        builder.build("test", Opts(), stateInit = { mutableMapOf() })
    }
}
