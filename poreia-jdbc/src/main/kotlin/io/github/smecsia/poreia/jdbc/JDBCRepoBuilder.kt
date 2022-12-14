package io.github.smecsia.poreia.jdbc

import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.processing.Locker
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.core.api.serialize.ToBytesSerializer
import io.github.smecsia.poreia.jdbc.dialect.BasicDialect
import io.github.smecsia.poreia.jdbc.dialect.Dialect
import io.github.smecsia.poreia.jdbc.util.FSTStateSerializer
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
        stateClass: Class<S>?,
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
