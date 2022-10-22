package org.poreia.jdbc

import org.junit.rules.ExternalResource
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.MySQLContainerProvider
import org.testcontainers.containers.PostgreSQLContainerProvider

class JDBCRule(
    private val postgres: JdbcDatabaseContainer<*> = PostgreSQLContainerProvider().newInstance(),
    private val mysql: JdbcDatabaseContainer<*> = MySQLContainerProvider().newInstance(),
) : ExternalResource() {

    override fun before() {
        mysql.start()
        postgres.start()
    }

    override fun after() {
        postgres.stop()
        mysql.stop()
    }

    val mysqlJDBCUrl: String by lazy {
        mysql.jdbcUrl
    }
    val postgresJDBCUrl: String by lazy {
        postgres.jdbcUrl
    }
}
