package enki.pm.project

import cats.free.Free
import cats._
import cats.implicits._
import enki.pm.fs._
import io.chrisdavenport.log4cats.Logger

sealed trait ModuleTree[F[_]] {
  def build(implicit m: Monad[F]): F[ModuleTree[Id]]
}

trait Submodules[F[_]] extends ModuleTree[F] {
  def modules: F[Seq[ModuleTree[F]]]

  override def build(implicit m: Monad[F]): F[ModuleTree[Id]] = modules >>= {
    _.toList.traverse(_.build).map { m =>
      new Submodules[Id] {
        override def modules: List[ModuleTree[Id]] = m
      }.asInstanceOf[ModuleTree[Id]]
    }
  }
}

trait SqlModule[F[_]] extends ModuleTree[F] {
}

object ModuleTree {
  def fromDir[F[_] : Monad](dir: Dir[F])(implicit logger: Logger[F]): Submodules[F] = new Submodules[F] {
    override def modules: F[Seq[ModuleTree[F]]] = for {
      aaa <- logger.info("test") //TODO: просканировать папки, найти SQL файлы.
    } yield Seq.empty[ModuleTree[F]]
  }
}
