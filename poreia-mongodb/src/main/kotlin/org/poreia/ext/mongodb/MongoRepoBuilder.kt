package org.poreia.ext.mongodb

import com.mongodb.client.MongoClient
import org.poreia.core.api.Opts
import org.poreia.core.api.processing.RepoBuilder
import org.poreia.core.api.processing.Repository
import org.poreia.core.api.processing.StateInitializer
import org.poreia.ext.mongodb.core.LockingSupport.LockOpts
import org.poreia.ext.mongodb.core.MongoLockingSupport

class MongoRepoBuilder<S : Any>(
    private val mongo: MongoClient,
    private val dbName: String,
    private val serializer: ToBsonSerializer<S> = DefaultToBsonSerializer(),
    private val repoCollection: (name: String) -> String = { "repo_$it" },
    private val locksCollection: (name: String) -> String = { "locks_$it" }
) : RepoBuilder<S> {

    @Suppress("UNCHECKED_CAST")
    override fun build(
        name: String,
        opts: Opts,
        stateInit: StateInitializer<S>?,
        stateClass: Class<S>?
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
                    lockTimeoutMs = opts.maxLockWaitMs
                )
            ),
            stateInitializer = stateInit
        )
    }
}
