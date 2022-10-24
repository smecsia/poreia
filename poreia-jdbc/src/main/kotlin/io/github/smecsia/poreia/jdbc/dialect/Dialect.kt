package io.github.smecsia.poreia.jdbc.dialect

import java.sql.Connection
import java.sql.SQLException

interface Dialect {
    @Throws(SQLException::class)
    fun createLocksTableIfNotExists(tableName: String, conn: Connection)

    @Throws(SQLException::class)
    fun createRepoTableIfNotExists(tableName: String, conn: Connection)

    @Throws(SQLException::class)
    fun tryLock(tableName: String, key: String, conn: Connection)

    @Throws(SQLException::class)
    fun tryUnlock(tableName: String, key: String, conn: Connection)

    @Throws(SQLException::class)
    fun forceUnlock(tableName: String, key: String, conn: Connection)

    @Throws(SQLException::class)
    fun isLocked(tableName: String, key: String, conn: Connection): Boolean

    @Throws(SQLException::class)
    fun isLockedByMe(tableName: String, key: String, conn: Connection): Boolean

    @Throws(SQLException::class)
    fun put(tableName: String, key: String, conn: Connection, bytes: ByteArray)

    @Throws(SQLException::class)
    fun remove(tableName: String, key: String, conn: Connection)

    @Throws(SQLException::class)
    fun clear(tableName: String, conn: Connection)

    @Throws(SQLException::class)
    operator fun get(tableName: String, key: String, conn: Connection): ByteArray?

    @Throws(SQLException::class)
    fun keys(tableName: String, conn: Connection): Collection<String>

    @Throws(SQLException::class)
    fun valuesMap(tableName: String, conn: Connection): Map<String, ByteArray>
}
