package enki
package readers

object default {
  implicit val defaultReader: Reader = new TableReader {
    override protected def getTableName(name: Symbol): String = name.name
  }
}
