package org.poreia.ext.mongodb.core

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.awaitility.Awaitility.await
import org.bson.Document
import org.bson.types.ObjectId
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.hasItems
import org.hamcrest.MatcherAssert.assertThat
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.poreia.ext.mongodb.util.MongoDbRule
import java.lang.Thread.sleep
import java.net.UnknownHostException
import java.time.Instant.now
import java.util.Date
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit.SECONDS

class MongoQueueCoreTest {
    companion object {
        @ClassRule
        @JvmField
        val mongodb = MongoDbRule()
    }

    private lateinit var collection: MongoCollection<Document>
    private lateinit var queue: MongoQueueCore
    private lateinit var db: MongoDatabase
    private lateinit var client: MongoClient

    @Before
    fun setup() {
        client = mongodb.client
        db = client.getDatabase("queue${randomUUID()}")
        collection = db.getCollection("test${randomUUID()}")
        queue = MongoQueueCore(collection)
    }

    @After
    fun closeConnection() {
        db.drop()
    }

    @Test
    fun ensureGetIndex() {
        queue.ensureGetIndex(Document("type", 1).append("boo", -1))
        queue.ensureGetIndex(Document("another.sub", 1))
        val indexes = collection.listIndexes().toList()

        assertThat(indexes.size, equalTo(4))
        val expectedOne = Document("running", 1)
            .append("payload.type", 1)
            .append("priority", 1)
            .append("created", 1)
            .append("payload.boo", -1)
            .append("earliestGet", 1)
        assertEquals(expectedOne, indexes[1]["key"])
        val expectedTwo = Document("running", 1).append("resetTimestamp", 1)
        assertEquals(expectedTwo, indexes[2]["key"])
        val expectedThree = Document("running", 1)
            .append("payload.another.sub", 1)
            .append("priority", 1)
            .append("created", 1)
            .append("earliestGet", 1)
        assertEquals(expectedThree, indexes[3]["key"])
    }

    @Test
    fun ensureGetIndex_noArgs() {
        queue.ensureGetIndex()
        val indexes = collection.listIndexes().toList()
        assertEquals(3, indexes.size)
        val expectedOne = Document("running", 1).append("priority", 1).append("created", 1).append("earliestGet", 1)
        assertEquals(expectedOne, indexes[1]["key"])
        val expectedTwo = Document("running", 1).append("resetTimestamp", 1)
        assertEquals(expectedTwo, indexes[2]["key"])
    }

    @Test(expected = RuntimeException::class)
    @Throws(UnknownHostException::class)
    fun ensureGetIndex_tooLongCollectionName() {
        // 121 chars
        val collectionName = ("messages01234567890123456789012345678901234567890123456789" +
                "012345678901234567890123456789012345678901234567890123456789012")
        queue = MongoQueueCore(db.getCollection(collectionName))
        queue.ensureGetIndex()
    }

    @Test(expected = IllegalArgumentException::class)
    fun ensureGetIndex_badBeforeSortValue() {
        queue.ensureGetIndex(Document("field", "NotAnInt"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun ensureGetIndex_badAfterSortValue() {
        queue.ensureGetIndex(Document(), Document("field", "NotAnInt"))
    }

    @Test
    fun ensureCountIndex() {
        queue.ensureCountIndex(Document("type", 1).append("boo", -1), false)
        queue.ensureCountIndex(Document("another.sub", 1), true)
        val indexes = collection.listIndexes().toList()
        assertEquals(3, indexes.size.toLong())
        val expectedOne = Document("payload.type", 1).append("payload.boo", -1)
        assertEquals(expectedOne, indexes[1]["key"])
        val expectedTwo = Document("running", 1).append("payload.another.sub", 1)
        assertEquals(expectedTwo, indexes[2]["key"])
    }

    @Test(expected = IllegalArgumentException::class)
    fun ensureCountIndex_badValue() {
        queue.ensureCountIndex(Document("field", "NotAnInt"), true)
    }

    @Test
    fun get_badQuery() {
        queue.send(Document("key", 0))
        assertNull(queue[Document("key", 1), Int.MAX_VALUE, 0])
    }

    @Test
    fun get_fullQuery() {
        val message = Document("id", "ID SHOULD BE REMOVED").append("key1", 0).append("key2", true)
        queue.send(message)
        queue.send(Document())
        val result = queue[message, Int.MAX_VALUE]
        assertNotEquals(message["id"], result!!["id"])
        message["id"] = result["id"]
        assertEquals(message, result)
    }

    @Test
    fun get_subQuery() {
        val messageOne = Document("one", Document("two", Document("three", 5)))
        val messageTwo = Document("one", Document("two", Document("three", 4)))
        queue.send(messageOne)
        queue.send(messageTwo)
        val result = queue[Document("one.two.three", Document("\$gte", 5)), Int.MAX_VALUE]!!
        messageOne["id"] = result["id"]
        assertEquals(messageOne, result)
    }

    @Test
    fun get_negativeWait() {
        assertNull(queue[Document(), Int.MAX_VALUE, Long.MIN_VALUE])
        queue.send(Document())
        assertNotNull(queue[Document(), Int.MAX_VALUE, Long.MIN_VALUE])
    }

    @Test
    fun get_negativePoll() {
        assertNull(queue[Document(), Int.MAX_VALUE, 100, Long.MIN_VALUE])
        queue.send(Document())
        assertNotNull(queue[Document(), Int.MAX_VALUE, 100, Long.MIN_VALUE])
    }

    @Test
    fun get_beforeAck() {
        queue.send(Document())
        assertNotNull(queue[Document(), Int.MAX_VALUE, 200])

        // try get message we already have before ack
        assertNull(queue[Document(), Int.MAX_VALUE, 200])
    }

    @Test
    fun get_customPriority() {
        val messageOne = Document("key", 1)
        val messageTwo = Document("key", 2)
        val messageThree = Document("key", 3)
        queue.send(messageOne, priority = 0.5)
        queue.send(messageTwo, priority = 0.4)
        queue.send(messageThree, priority = 0.3)
        val resultOne = queue.get(timeoutMs = 200)
        val resultTwo = queue.get(timeoutMs = 200)
        val resultThree = queue.get(timeoutMs = 200)
        assertEquals(messageOne["key"], resultThree!!["key"])
        assertEquals(messageTwo["key"], resultTwo!!["key"])
        assertEquals(messageThree["key"], resultOne!!["key"])
    }

    @Test
    fun get_timePriority() {
        val messageOne = Document("key", 1)
        val messageTwo = Document("key", 2)
        val messageThree = Document("key", 3)
        queue.send(messageOne)
        queue.send(messageTwo)
        queue.send(messageThree)
        val resultOne = queue.get()
        val resultTwo = queue.get()
        val resultThree = queue.get()
        assertEquals(messageOne["key"], resultOne!!["key"])
        assertEquals(messageTwo["key"], resultTwo!!["key"])
        assertEquals(messageThree["key"], resultThree!!["key"])
    }

    @Test
    fun get_wait() {
        val start = Date()
        queue[Document(), Int.MAX_VALUE, 200]
        val elapsed = Date().time - start.time
        assertTrue(elapsed >= 200)
        assertTrue(elapsed < 400)
    }

    @Test
    fun get_waitWhenMessageExists() {
        val start = Date()
        queue.send(Document())
        queue[Document(), Int.MAX_VALUE, 3000]
        assertTrue(Date().time - start.time < 2000)
    }

    @Test
    fun get_earliestGet() {
        queue.send(Document(), earliestGet = now().plusMillis(200))
        assertNull(queue[Document(), Int.MAX_VALUE, 0])
        sleep(200)
        assertNotNull(queue[Document(), Int.MAX_VALUE])
    }

    @Test
    fun get_resetStuck() {
        queue.send(Document())

        // sets resetTimestamp on messageOne
        assertNotNull(queue[Document(), 0])
        assertNotNull(queue[Document(), Int.MAX_VALUE])
    }

    @Test
    fun count_running() {
        assertEquals(0, queue.count(true))
        assertEquals(0, queue.count(false))
        assertEquals(0, queue.count())
        queue.send(Document("key", 1))
        assertEquals(0, queue.count(true))
        assertEquals(1, queue.count(false))
        assertEquals(1, queue.count())
        queue[Document(), Int.MAX_VALUE, 200]
        assertEquals(1, queue.count(true))
        assertEquals(0, queue.count(false))
        assertEquals(1, queue.count())
    }

    @Test
    fun count_fullQuery() {
        val message = Document("key", 1)
        queue.send(Document())
        queue.send(message)
        assertEquals(1, queue.count(query = message))
    }

    @Test
    fun count_subQuery() {
        val messageOne = Document("one", Document("two", Document("three", 4)))
        val messageTwo = Document("one", Document("two", Document("three", 5)))
        queue.send(messageOne)
        queue.send(messageTwo)
        assertEquals(1, queue.count(query = Document("one.two.three", Document("\$gte", 5))))
    }

    @Test
    fun count_badQuery() {
        queue.send(Document("key", 0))
        assertEquals(0, queue.count(query = Document("key", 1)))
    }

    @Test
    fun ack() {
        val message = Document("key", 0)
        queue.send(message)
        queue.send(Document())
        val result = queue[message, Int.MAX_VALUE]!!
        assertEquals(2, collection.countDocuments())
        queue.ack(result)
        assertEquals(1, collection.countDocuments())
    }

    @Test(expected = IllegalArgumentException::class)
    fun ack_wrongIdType() {
        queue.ack(Document("id", false))
    }

    @Test
    fun ackSend() {
        val message = Document("key", 0)
        queue.send(message)
        val resultOne = queue[message, Int.MAX_VALUE]
        val expectedEarliestGet = now()
        val expectedPriority = 0.8
        val timeBeforeAckSend = Date()
        queue.ackSend(resultOne!!, Document("key", 1), expectedEarliestGet, expectedPriority)
        assertEquals(1, collection.countDocuments())

        // find one, i.e. the first document in this collection
        val actual = collection.find().first()
        val actualCreated = actual?.getDate("created")
        assertTrue(actualCreated!! >= timeBeforeAckSend && actualCreated <= Date())
        val expected = Document("_id", resultOne["id"])
            .append("payload", Document("key", 1))
            .append("running", false)
            .append("resetTimestamp", Int.MAX_VALUE.toLong())
            .append("earliestGet", expectedEarliestGet.toEpochMilli())
            .append("priority", expectedPriority)
            .append("created", actual["created"])
        assertEquals(expected, actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun ackSend_wrongIdType() {
        queue.ackSend(Document("id", 5), Document())
    }

    @Test(expected = IllegalArgumentException::class)
    fun ackSend_nanPriority() {
        queue.ackSend(Document("id", ObjectId.get()), Document(), now(), Double.NaN)
    }

    @Test
    fun requeue() {
        val message = Document("key", 0)
        queue.send(message)
        val resultOne = queue[message, Int.MAX_VALUE]
        val expectedEarliestGet = now()
        val expectedPriority = 0.8
        val timeBeforeRequeue = Date()
        queue.requeue(resultOne!!, expectedEarliestGet, expectedPriority)
        assertEquals(1, collection.countDocuments())

        // find one, i.e. the first document in this collection
        val actual = collection.find().first()!!
        val actualCreated = actual.getDate("created")
        assertTrue(actualCreated >= timeBeforeRequeue && actualCreated <= Date())
        val expected = Document("_id", resultOne["id"])
            .append("payload", Document("key", 0))
            .append("running", false)
            .append("resetTimestamp", Int.MAX_VALUE.toLong())
            .append("earliestGet", expectedEarliestGet.toEpochMilli())
            .append("priority", expectedPriority)
            .append("created", actual["created"])
        assertEquals(expected, actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun requeue_wrongIdType() {
        queue.requeue(Document("id", Document()))
    }

    @Test(expected = IllegalArgumentException::class)
    fun requeue_nanPriority() {
        queue.requeue(Document("id", ObjectId.get()), now(), Double.NaN)
    }

    @Test
    fun send() {
        val message = Document("key", 0)
        val expectedEarliestGet = now()
        val expectedPriority = 0.8
        val timeBeforeSend = Date()
        queue.send(message, expectedEarliestGet, expectedPriority)
        assertEquals(1, collection.countDocuments())

        // find one, i.e. the first document in this collection
        val actual = collection.find().first()!!
        val actualCreated = actual.getDate("created")
        assertTrue(actualCreated >= timeBeforeSend && actualCreated <= Date())
        val expected = Document("_id", actual["_id"])
            .append("payload", Document("key", 0))
            .append("running", false)
            .append("resetTimestamp", Int.MAX_VALUE.toLong())
            .append("earliestGet", expectedEarliestGet.toEpochMilli())
            .append("priority", expectedPriority)
            .append("created", actual["created"])
        assertEquals(expected, actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun send_nanPriority() {
        queue.send(Document("id", ObjectId.get()), now(), Double.NaN)
    }

    @Test
    fun `it should work with multiple queues`() {
        val queue1 = MongoQueueCore(db.getCollection("test_1_${randomUUID()}"))
        val queue2 = MongoQueueCore(db.getCollection("test_2_${randomUUID()}"))
        val result1 = mutableListOf<String>()
        val result2 = mutableListOf<String>()
        listOf(queue1 to result1, queue2 to result2).forEach { q ->
            Thread {
                while (true) {
                    try {
                        q.first.get()?.let { msg ->
                            q.second.add(msg["name"] as String)
                        }
                    } catch (ignored: Throwable) {
                    }
                }
            }.start()
        }
        repeat(5) {
            queue1.send(Document(mapOf("name" to "vasya$it")))
            queue2.send(Document(mapOf("name" to "petya$it")))
        }
        await().atMost(3, SECONDS)
            .until({ result1 }, hasItems("vasya0", "vasya1", "vasya2", "vasya3", "vasya4"))
        await().atMost(3, SECONDS)
            .until({ result2 }, hasItems("petya0", "petya1", "petya2", "petya3", "petya4"))
    }
}
