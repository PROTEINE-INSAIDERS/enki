package enki.source

object default {
  implicit val source: Source = new Source {
    override def qualifiedName(name: Symbol): Symbol = name
  }
}
