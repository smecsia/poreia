package io.github.smecsia.poreia.core.api.processing

typealias StateInitializer<S> = () -> S
typealias StateClosureFnc<S> = (String, S) -> Unit

/**
 * Base interface for state repositories
 */
interface Repository<S> : Locker {
    /**
     * Initializes new state for when it's empty
     */
    val stateInitializer: StateInitializer<S>?

    /**
     * Gets value without locking
     */
    operator fun get(key: String): S?

    /**
     * Locks key and returns the value
     * This method blocks until timeout if lock is not available
     *
     * @throws LockWaitTimeoutException if lock wait timeout is elapsed
     */
    @Throws(InterruptedException::class)
    fun lockAndGet(key: String): S?

    /**
     * Putting key to map without unlocking it
     */
    operator fun set(key: String, value: S): S

    /**
     * Returns key set
     */
    fun keys(): Collection<String>

    /**
     * Returns values map
     */
    fun values(): Map<String, S>

    /**
     * Putting key to map and unlocking it
     */
    fun setAndUnlock(key: String, value: S): S

    /**
     * Deleting key and unlocking it
     */
    fun deleteAndUnlock(key: String)

    /**
     * Clears repository from all the data
     */
    fun clear()

    /**
     * Perform operation against each state (performing lockAndGet and putAndUnlock then)
     *
     * @throws LockWaitTimeoutException if lock wait timeout is elapsed
     */
    @Throws(InterruptedException::class)
    fun withEach(closure: StateClosureFnc<S>) {
        values().forEach { (k, _) -> with(k, closure) }
    }

    /**
     * Perform operation against state for key (performing lockAndGet and putAndUnlock then)
     *
     * @Map?throws LockWaitTimeoutException if lock wait timeout is elapsed
     */
    @Throws(InterruptedException::class)
    fun with(key: String, closure: StateClosureFnc<S>): S {
        try {
            val state = lockAndGet(key) ?: stateInitializer?.let { it() }
                ?: throw IllegalStateException("No state found for '$key'")
            closure.invoke(key, state)
            return setAndUnlock(key, state)
        } catch (e: Exception) {
            unlock(key)
            throw e
        }
    }
}
