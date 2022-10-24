package io.github.smecsia.poreia.ext.mongodb.core

import io.github.smecsia.poreia.ext.mongodb.Address
import io.github.smecsia.poreia.ext.mongodb.User
import io.github.smecsia.poreia.ext.mongodb.core.MongoTailableCursorQueue.QueueOpts
import io.github.smecsia.poreia.ext.mongodb.util.MongoDbRule
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.core.Is.`is`
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.UUID.randomUUID
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

class MongoTailableQueueTest {
    @Rule
    @JvmField
    var mongodb: MongoDbRule = MongoDbRule()

    private lateinit var queue: MongoTailableCursorQueue<User>

    @Before
    fun setUp() {
        queue = MongoTailableCursorQueue<User>(
            mongodb.client,
            randomUUID().toString(),
            randomUUID().toString(),
            opts = QueueOpts(maxSize = 3),
        ).init()
    }

    @Test
    fun `it should allow initialize multiple times without breaking the queue`() {
        repeat(3) {
            queue.init()
            queue.add(user("vasya"))
        }
        assertThat(queue.size(), `is`(3L))
    }

    @Test
    fun `it should keep only newest messages respecting max size`() {
        repeat(5) { n: Int -> queue.add(user("Vasya$n")) }
        assertThat(queue.size(), `is`(3L))
        val users: MutableList<String> = ArrayList()
        newSingleThreadExecutor().submit {
            queue.poll(
                Consumer { u: User ->
                    users.add("${u.firstName!!}-${u.address?.location}")
                },
            )
        }
        await().atMost(1, SECONDS)
            .until<List<String>>({ users }, contains("Vasya2-loc", "Vasya3-loc", "Vasya4-loc"))
    }

    @Test
    fun `it should cumulatively accumulate messages from the queue`() {
        val users: MutableList<String> = ArrayList()
        newSingleThreadExecutor().submit {
            queue.poll(
                Consumer { u: User ->
                    users.add(u.firstName!!.toString())
                },
            )
        }
        repeat(2) { n: Int -> queue.add(user("Vasya$n")) }
        await().atMost(1, SECONDS)
            .until<List<String>>({ users }, contains("Vasya0", "Vasya1"))
        repeat(2) { n: Int -> queue.add(user("Vasya${n + 2}")) }
        await().atMost(1, SECONDS)
            .until<List<String>>({ users }, contains("Vasya0", "Vasya1", "Vasya2", "Vasya3"))
    }

    @Test
    fun `it should work with value with int`() {
        val sum = AtomicInteger()
        val queue = MongoTailableCursorQueue<Value>(
            mongodb.client,
            "test",
            "ints",
            opts = QueueOpts(maxSize = 10),
        ).init()

        newSingleThreadExecutor().submit {
            queue.poll(Consumer { sum.getAndAdd(it.value) })
        }
        repeat(5) { queue.add(Value(1)) }
        await().atMost(2, SECONDS)
            .until({ sum.get() }, `is`(5))
    }

    companion object {
        data class Value(var value: Int = 0)
    }

    private fun user(
        firstName: String?,
        address: Address = Address("loc"),
    ): User {
        val res = User(firstName = firstName, address = address)
        res.firstName = firstName
        return res
    }
}
