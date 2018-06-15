package enki
package sources

object default {
  implicit val defaultSource: Source = new TableSource {
    override protected def resolveTableName(name: Symbol): String = name.name

    override def qualifiedName(name: Symbol): Symbol = name
  }
}
