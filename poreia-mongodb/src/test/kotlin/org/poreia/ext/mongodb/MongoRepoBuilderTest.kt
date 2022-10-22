package org.poreia.ext.mongodb

import org.junit.Rule
import org.junit.Test
import org.poreia.core.SimpleState
import org.poreia.core.api.Opts
import org.poreia.ext.mongodb.util.MongoDbRule

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
