package enki.pm.fs

sealed trait DirTree[F[_]]

//TODO: тут, возможно, не стоит делать дерево, а обойтись F-алгеброй с функциями для работы с папкой.
trait Dir[F[_]] extends DirTree[F] {
  def contents: F[Seq[DirTree[F]]]
}

trait File[F[_]] extends DirTree[F] {

}