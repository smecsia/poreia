package io.github.smecsia.poreia.jdbc.dialect

import io.github.smecsia.poreia.core.util.ThreadUtil.threadId
import java.sql.Connection
import java.sql.SQLException

class PostgresDialect : BasicDialect() {
    override fun field(name: String): String = keyword(name)

    override fun table(name: String): String = keyword(name)

    private fun keyword(name: String) = "\"${name.replace("-", "_")}\""

    override fun createRepoSQL(tableName: String): String =
        """
            CREATE TABLE IF NOT EXISTS ${table(tableName)} (
              ${field("key")} VARCHAR(512),
              ${field("object")} BYTEA,
              PRIMARY KEY (${field("key")})
            )
        """

    override fun insertLockSQL(tableName: String, key: String?): String =
        """
            INSERT INTO ${table(tableName)} (${field("key")}, ${field("thread_id")})
                    VALUES ('$key', '${threadId()}')
        """

    override fun upsertSQL(tableName: String): String =
        """
            INSERT INTO ${table(tableName)} (${field("key")}, ${field("object")})
            VALUES (?, ?)
            ON CONFLICT (${field("key")}) DO UPDATE
                SET ${field("key")} = ?, ${field("object")} = ?
        """

    override fun createLocksSQL(tableName: String): String =
        """
            CREATE TABLE IF NOT EXISTS ${table(tableName)} (
              ${field("key")} VARCHAR(512),
              ${field("locked_date")} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              ${field("thread_id")} VARCHAR(256),
              PRIMARY KEY (${field("key")})
            )
        """.trimIndent()

    @Throws(SQLException::class)
    override fun put(tableName: String, key: String, conn: Connection, bytes: ByteArray) {
        val statement = conn.prepareStatement(upsertSQL(tableName))
        statement.setString(1, key)
        statement.setBytes(2, bytes)
        statement.setString(3, key)
        statement.setBytes(4, bytes)
        statement.executeUpdate()
    }
}
