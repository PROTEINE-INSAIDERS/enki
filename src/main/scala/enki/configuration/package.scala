package enki

package object configuration {
  private[enki] val enkiKey = "enki"

  private[enki] def resolveTableName(schema: Option[String], table: Symbol): String = {
    schema.map(schemaName => s"$schemaName.${table.name}").getOrElse(table.name)
  }
}