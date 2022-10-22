package org.poreia.ext.mongodb

import java.io.Serializable

data class User(
    var firstName: String? = null,
    var address: Address? = null,
    var lastName: String? = null
) : Serializable

data class Address(
    var location: String? = null,
    var name: String? = null
)
