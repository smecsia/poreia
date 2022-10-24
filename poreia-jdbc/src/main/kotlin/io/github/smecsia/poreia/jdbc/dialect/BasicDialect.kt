package io.github.smecsia.poreia.jdbc.dialect

import io.github.smecsia.poreia.core.util.ThreadUtil.threadId
import java.sql.Connection
import java.sql.SQLException

open class BasicDialect : Dialect {
    @Throws(SQLException::class)
    override fun createLocksTableIfNotExists(tableName: String, conn: Connection) {
        conn.createStatement().execute(createLocksSQL(tableName))
    }

    @Throws(SQLException::class)
    override fun createRepoTableIfNotExists(tableName: String, conn: Connection) {
        conn.createStatement().execute(createRepoSQL(tableName))
    }

    @Throws(SQLException::class)
    override fun tryLock(tableName: String, key: String, conn: Connection) {
        conn.createStatement().execute(insertLockSQL(tableName, key))
    }

    @Throws(SQLException::class)
    override fun tryUnlock(tableName: String, key: String, conn: Connection) {
        conn.createStatement().execute(removeLockSQL(tableName, key))
    }

    @Throws(SQLException::class)
    override fun forceUnlock(tableName: String, key: String, conn: Connection) {
        conn.createStatement().execute(forceRemoveLockSQL(tableName, key))
    }

    @Throws(SQLException::class)
    override fun isLocked(tableName: String, key: String, conn: Connection): Boolean = conn.createStatement().let {
        it.execute("SELECT * FROM ${table(tableName)} WHERE ${field("key")}='$key'")
        it.resultSet.next()
    }

    @Throws(SQLException::class)
    override fun isLockedByMe(tableName: String, key: String, conn: Connection): Boolean = conn.createStatement().let {
        it.execute(
            "SELECT * FROM ${table(tableName)} " +
                "WHERE ${field("key")}='$key' AND ${field("thread_id")} ='${threadId()}'",
        )
        it.resultSet.next()
    }

    @Throws(SQLException::class)
    override fun put(tableName: String, key: String, conn: Connection, bytes: ByteArray) {
        val statement = conn.prepareStatement(upsertSQL(tableName))
        statement.setString(1, key)
        statement.setBytes(2, bytes)
        statement.executeUpdate()
    }

    @Throws(SQLException::class)
    override fun remove(tableName: String, key: String, conn: Connection) {
        val statement = conn.createStatement()
        statement.executeUpdate("DELETE FROM ${table(tableName)} WHERE ${field("key")} = '$key'")
    }

    @Throws(SQLException::class)
    override fun clear(tableName: String, conn: Connection) {
        conn.createStatement().executeUpdate("DELETE FROM " + table(tableName))
    }

    @Throws(SQLException::class)
    override fun get(tableName: String, key: String, conn: Connection): ByteArray? {
        val statement = conn.createStatement()
        statement.execute("SELECT object FROM ${table(tableName)} WHERE ${field("key")} = '$key'")
        return if (statement.resultSet.next()) statement.resultSet.getBytes("object") else null
    }

    @Throws(SQLException::class)
    override fun keys(tableName: String, conn: Connection): Collection<String> {
        val statement = conn.createStatement()
        statement.execute("SELECT ${field("key")} FROM ${table(tableName)}")
        val res = mutableListOf<String>()
        while (statement.resultSet.next()) {
            res += statement.resultSet?.getString("key")!!
        }
        return res
    }

    @Throws(SQLException::class)
    override fun valuesMap(tableName: String, conn: Connection): Map<String, ByteArray> {
        val statement = conn.createStatement()
        statement.execute("SELECT ${field("key")}, ${field("object")} FROM ${table(tableName)}")
        val res = mutableMapOf<String, ByteArray>()
        while (statement.resultSet.next()) {
            res[statement.resultSet.getString("key")] = statement.resultSet.getBytes("object")
        }
        return res
    }

    protected open fun table(name: String): String = name

    protected open fun field(name: String): String = "`$name`"

    protected open fun upsertSQL(tableName: String): String =
        """
             MERGE INTO ${table(tableName)}(${field("key")}, ${field("object")}) VALUES (?, ?)
        """.trimIndent()

    protected open fun insertLockSQL(tableName: String, key: String?): String =
        """
            INSERT INTO ${table(tableName)} (${field("key")}, ${field("locked_date")}, ${field("thread_id")})
                    VALUES ('$key', current_date() - 100, '${threadId()}')
        """.trimIndent()

    protected open fun createLocksSQL(tableName: String): String =
        """
            CREATE TABLE IF NOT EXISTS ${table(tableName)} (
              ${field("key")} VARCHAR(512),
              ${field("locked_date")} DATE,
              ${field("thread_id")} VARCHAR(256),
              PRIMARY KEY (${field("key")})
            )
        """.trimIndent()

    protected open fun createRepoSQL(tableName: String): String =
        """
            CREATE TABLE IF NOT EXISTS ${table(tableName)} (
              ${field("key")} VARCHAR(512),
              ${field("object")} VARBINARY($maxObjectSize),
              PRIMARY KEY (${field("key")})
            )
        """.trimIndent()

    protected fun removeLockSQL(tableName: String, key: String): String =
        """
            DELETE FROM ${table(tableName)} WHERE ${field("key")}='$key' AND ${field("thread_id")}='${threadId()}'
        """.trimIndent()

    protected fun forceRemoveLockSQL(tableName: String, key: String): String =
        """
            DELETE FROM ${table(tableName)} WHERE ${field("key")}='$key'
        """.trimIndent()

    private val maxObjectSize = 30720
}
