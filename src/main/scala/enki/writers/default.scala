package enki.writers

object default {
  implicit val defaultWriter: Writer = new TableWriter {
    override protected def getTableName(table: Symbol): String = table.name
  }
}
