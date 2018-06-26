package enki.writers

object default {
  implicit val defaultWriter: Writer = new TableWriter {
    override protected def tableName(table: Symbol): String = table.name
  }
}
