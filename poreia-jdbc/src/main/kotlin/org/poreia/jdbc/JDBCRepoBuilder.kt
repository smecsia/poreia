package org.poreia.jdbc

import org.poreia.core.api.Opts
import org.poreia.core.api.processing.Locker
import org.poreia.core.api.processing.RepoBuilder
import org.poreia.core.api.processing.Repository
import org.poreia.core.api.processing.StateInitializer
import org.poreia.core.api.serialize.ToBytesSerializer
import org.poreia.jdbc.dialect.BasicDialect
import org.poreia.jdbc.dialect.Dialect
import org.poreia.jdbc.util.FSTStateSerializer
import java.sql.Connection

class JDBCRepoBuilder<S>(
    val connection: Connection,
    private val serializer: ToBytesSerializer<S> = FSTStateSerializer(),
    val dialect: Dialect = BasicDialect(),
    private val locking: Locker = JDBCPessimisticLocking(connection = connection, dialect = dialect),
) : RepoBuilder<S> {

    override fun build(
        name: String,
        opts: Opts,
        stateInit: StateInitializer<S>?,
        stateClass: Class<S>?
    ): Repository<S> {
        return JDBCRepo(
            connection = connection,
            tableName = name.replace("-", "_"),
            locking = locking,
            dialect = dialect,
            serializer = serializer,
            stateInitializer = stateInit,
        )
    }
}