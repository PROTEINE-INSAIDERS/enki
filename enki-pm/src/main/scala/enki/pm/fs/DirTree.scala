package enki.pm.fs

sealed trait DirTree[F[_]]

trait Dir[F[_]] extends DirTree[F] {
  def contents: F[Seq[DirTree[F]]]
}

trait File[F[_]] extends DirTree[F] {

}