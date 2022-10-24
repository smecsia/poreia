package io.github.smecsia.poreia.jdbc.dialect

class MysqlDialect : BasicDialect() {
    override fun field(name: String): String = "`$name`"

    override fun table(name: String): String = "`$name`"

    override fun upsertSQL(tableName: String): String = """
        INSERT INTO ${table(tableName)} (${field("key")}, ${field("object")})
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE
          ${field("key")} = VALUES(${field("key")}),
          ${field("object")} = VALUES(${field("object")})
    """

    override fun createLocksSQL(tableName: String): String = """
        CREATE TABLE IF NOT EXISTS ${table(tableName)} (
          ${field("key")} VARCHAR(512),
          ${field("locked_date")} DATETIME,
          ${field("thread_id")} VARCHAR(256),
          PRIMARY KEY (${field("key")})
        )
    """
}
