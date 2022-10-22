package org.poreia.jdbc

import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.poreia.core.basicPipeline
import java.util.concurrent.TimeUnit.SECONDS

@RunWith(Parameterized::class)
class JDBCRepoTest : BaseJDBCRepoTest() {

    @Test
    fun `JDBC repository should work and aggregate`() {
        val pipeline = basicPipeline {
            repoBuilder = jdbcRepoBuilder
            process("input").output("users")
            aggregateTo("users", key = { "all" }, aggregate = { s, m ->
                s["names"] = (s["names"]?.let { "${it}," } ?: "") + (m["name"] as String)
            })
        }
        pipeline.send("input", mapOf("name" to "Vasya", "lastName" to "Fedorov"))
        pipeline.send("input", mapOf("name" to "Petya", "lastName" to "Makarov"))
        pipeline.send("input", mapOf("name" to "Sergey", "lastName" to "Vasilyev"))

        await().atMost(5, SECONDS).until(
            { pipeline.repo("users")["all"]?.get("names")?.split(",") },
            { (it?.size ?: 0) >= 3 }
        )

        val names = pipeline.repo("users")["all"]?.get("names")?.split(",")
        assertThat(names, containsInAnyOrder("Vasya", "Petya", "Sergey"))
        pipeline.stop()
    }
}