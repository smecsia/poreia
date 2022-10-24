package io.github.smecsia.poreia.ext.serializer.jackson

import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import java.io.Serializable

class JacksonToBytesSerializerTest {
    @Test
    fun `it should use jackson message serializer`() {
        val petya = Person(
            name = "Petya",
            lastName = "Ivanov",
            age = 30,
            address = Address(street = "Lenina", number = 10),
        )
        val vasya = Person(
            name = "Vasya",
            lastName = "Petrov",
            age = 23,
            address = Address(street = "Moscow prospect", number = 120),
        )

        val serializer = JacksonToBytesSerializer<Person>()

        val serializedPetya = serializer.serialize(petya)
        val serializedVasya = serializer.serialize(vasya)
        println(serializedPetya)
        println(serializedVasya)

        assertThat(serializer.deserialize(serializedPetya), equalTo(petya))
        assertThat(serializer.deserialize(serializedVasya), equalTo(vasya))
    }

    data class Person(
        val name: String,
        val lastName: String,
        val age: Int,
        val address: Address,
    ) : Serializable

    data class Address(
        val street: String,
        val number: Int,
    )
}
