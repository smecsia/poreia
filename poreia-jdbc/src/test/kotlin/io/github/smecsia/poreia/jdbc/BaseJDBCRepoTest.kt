package io.github.smecsia.poreia.jdbc

import io.github.smecsia.poreia.core.State
import io.github.smecsia.poreia.jdbc.dialect.MysqlDialect
import io.github.smecsia.poreia.jdbc.dialect.PostgresDialect
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameter
import java.sql.DriverManager.getConnection

abstract class BaseJDBCRepoTest {
    @Parameter(0)
    lateinit var testName: String

    @Parameter(1)
    lateinit var jdbcRepoBuilder: JDBCRepoBuilder<State<String>>

    companion object {
        private const val H2_JDBC_URL = "jdbc:h2:mem:test_db;DB_CLOSE_DELAY=-1"
        private const val MYSQL_JDBC_URL = "jdbc:tc:mysql:5.7.34:///test_db"
        private const val PG_JDBC_URL = "jdbc:tc:postgresql:9.6.8:///test_db"

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun data(): List<Array<Any>> {
            return listOf(
                arrayOf(
                    "H2DB",
                    JDBCRepoBuilder<State<String>>(getConnection(H2_JDBC_URL)),
                ),
                arrayOf(
                    "MySQL",
                    JDBCRepoBuilder<State<String>>(getConnection(MYSQL_JDBC_URL), dialect = MysqlDialect()),
                ),
                arrayOf(
                    "PostgreSQL",
                    JDBCRepoBuilder<State<String>>(getConnection(PG_JDBC_URL), dialect = PostgresDialect()),
                ),
            )
        }
    }
}
