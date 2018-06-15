package enki.source

trait Source {
  def qualifiedName(name: Symbol): Symbol
}
