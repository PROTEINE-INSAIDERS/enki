package enki.pm
package project

import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import org.apache.commons.io._
import qq.droste._
import qq.droste.data._

case class Module(
                   name: String // а оно нам тут нужно?
                 )

object Module {
  type Coalgebra[F[_], A] = qq.droste.CoalgebraM[F, RoseTree[Module, ?], A]

  def moduleNameFromPath(path: Path): String = {
    FilenameUtils.removeExtension(path.getFileName.toString)
  }

  def fromFilesystem[M[_] : Monad](implicit fileSystem: FileSystem[M]): Coalgebra[M, Path] =
    CoalgebraM[M, CoattrF[List, Module, ?], Path] { path =>
      (fileSystem.isRegularFile(path), fileSystem.isDirectory(path)).tupled >>= {
        case (true, _) => CoattrF.pure[List, Module, Path](Module(moduleNameFromPath(path))).pure[M]
        case (_, true) => fileSystem.list(path) map CoattrF.roll[List, Module, Path]
        case _ => CoattrF.roll[List, Module, Path](List.empty).pure[M]
      }
    }
}