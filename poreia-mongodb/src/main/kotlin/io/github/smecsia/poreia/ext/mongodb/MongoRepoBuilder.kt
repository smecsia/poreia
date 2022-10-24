package io.github.smecsia.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import io.github.smecsia.poreia.core.api.Opts
import io.github.smecsia.poreia.core.api.processing.RepoBuilder
import io.github.smecsia.poreia.core.api.processing.Repository
import io.github.smecsia.poreia.core.api.processing.StateInitializer
import io.github.smecsia.poreia.ext.mongodb.core.LockingSupport.LockOpts
import io.github.smecsia.poreia.ext.mongodb.core.MongoLockingSupport

class MongoRepoBuilder<S : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: ToBsonSerializer<S> = DefaultToBsonSerializer(),
    private val repoCollection: (name: String) -> String = { "repo_$it" },
    private val locksCollection: (name: String) -> String = { "locks_$it" },
) : RepoBuilder<S> {

    @Suppress("UNCHECKED_CAST")
    override fun build(
        name: String,
        opts: Opts,
        stateInit: StateInitializer<S>?,
        stateClass: Class<S>?,
    ): Repository<S> {
        return MongoKVRepository(
            mongo = mongo,
            dbName = dbName,
            collection = repoCollection(name),
            serializer = serializer,
            locker = MongoLockingSupport(
                mongo = mongo,
                collection = locksCollection(name),
                dbName = dbName,
                opts = LockOpts(
                    lockTimeoutMs = opts.maxLockWaitMs,
                ),
            ),
            stateInitializer = stateInit,
        )
    }
}
