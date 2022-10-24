package io.github.smecsia.poreia.ext.mongodb

import com.nhaarman.mockitokotlin2.eq
import io.github.smecsia.poreia.core.Pipeline.Companion.startPipeline
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.TimeUnit.SECONDS

class MongoKVRepositoryTest {
    @Rule
    @JvmField
    var mongodb: MongoDbRule = MongoDbRule()

    private var repo: MongoKVRepository<User>? = null

    @Before
    fun setUp() {
        repo = MongoKVRepository(
            mongo = mongodb.client,
            dbName = "test",
            collection = "test",
        )
    }

    @Test
    fun `it should accumulate messages from the queue`() {
        val pipeline = startPipeline<String, User> {
            repoBuilder = MongoRepoBuilder(mongodb.client, "test")
            stateInit = { User() }
            stateClass = User::class.java
            process("input", process = { "$it Ivanovich" })
                .output("users")
            aggregateTo("users", key = { it }, aggregate = { s, m ->
                s.firstName = m
            },)
        }

        listOf("Ivan", "Alexey", "Petr").forEach {
            pipeline.send("input", it)
        }

        await().atMost(2, SECONDS).until(
            { pipeline.repo("users").keys() },
            containsInAnyOrder(
                "Ivan Ivanovich",
                "Alexey Ivanovich",
                "Petr Ivanovich",
            ),
        )
        assertThat(
            pipeline.repo("users").values().values,
            containsInAnyOrder(
                eq(User("Ivan Ivanovich")),
                eq(User("Alexey Ivanovich")),
                eq(User("Petr Ivanovich")),
            ),
        )

        pipeline.stop()
    }
}
