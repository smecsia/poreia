package io.github.smecsia.poreia.jdbc

import io.github.smecsia.poreia.core.api.processing.Locker
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import io.github.smecsia.poreia.core.error.LockWaitTimeoutException
import io.github.smecsia.poreia.jdbc.dialect.BasicDialect
import io.github.smecsia.poreia.jdbc.dialect.Dialect
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

class JDBCRepo<S>(
    private val connection: Connection,
    private val tableName: String,
    private val locking: Locker,
    private val dialect: Dialect = BasicDialect(),
    private val serializer: ToBytesSerializer<S>,
    override val stateInitializer: StateInitializer<S>?,
) : Repository<S> {

    init {
        dialect.createRepoTableIfNotExists(tableName, connection)
    }

    override operator fun get(key: String): S? {
        val data: ByteArray? = dialect[tableName, key, connection]
        return data?.let { serializer.deserialize(it) }
    }

    override fun isLockedByMe(key: String): Boolean = locking.isLockedByMe(key)

    override fun keys(): Collection<String> = dialect.keys(tableName, connection)

    override fun values(): Map<String, S> = dialect.valuesMap(tableName, connection).map {
        it.key to serializer.deserialize(it.value)
    }.toMap()

    @Throws(LockWaitTimeoutException::class)
    override fun lockAndGet(key: String): S? {
        locking.tryLock(key)
        return dialect[tableName, key, connection]?.let {
            serializer.deserialize(it)
        }
    }

    @Throws(LockWaitTimeoutException::class)
    override fun lock(key: String) {
        locking.tryLock(key)
    }

    override fun tryLock(key: String): Boolean = locking.tryLock(key)

    override fun unlock(key: String) {
        locking.unlock(key)
    }

    override fun setAndUnlock(key: String, value: S): S {
        dialect.put(tableName, key, connection, serializer.serialize(value))
        locking.unlock(key)
        return value
    }

    override fun forceUnlock(key: String) {
        locking.forceUnlock(key)
    }

    override fun set(key: String, value: S): S {
        dialect.put(tableName, key, connection, serializer.serialize(value))
        return value
    }

    override fun deleteAndUnlock(key: String) = dialect.remove(tableName, key, connection)

    override fun clear() = dialect.clear(tableName, connection)

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(JDBCRepo::class.java)
    }
}
