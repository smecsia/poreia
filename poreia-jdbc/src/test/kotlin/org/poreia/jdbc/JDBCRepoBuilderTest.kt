package org.poreia.jdbc

import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.poreia.core.State
import org.poreia.core.api.Opts
import org.poreia.core.api.processing.Repository

@RunWith(Parameterized::class)
class JDBCRepoBuilderTest : BaseJDBCRepoTest() {

    @Test
    fun `test JDBC repository builder should work`() {
        val repo: Repository<State<String>> = jdbcRepoBuilder.build("repo1", Opts(maxLockWaitMs = 500))

        repo["user"] = mutableMapOf("name" to "Vasya")
        assertThat(repo["user"], equalTo(mapOf("name" to "Vasya")))

        val user = repo.lockAndGet("user")!!
        Thread { repo.with("user") { _, u -> u["name"] = u["name"] + "+Petya" } }.start()
        Thread.sleep(100)
        user["name"] = "Masha"
        Thread.sleep(100)
        repo.setAndUnlock("user", user)
        Thread.sleep(300)
        assertThat(repo["user"], equalTo(mapOf("name" to "Masha+Petya")))
    }
}