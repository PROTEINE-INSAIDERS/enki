package enki.pm
package project

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import org.apache.commons.io._
import qq.droste._
import qq.droste.data._


sealed trait Module

final case class SqlModule(
                      sql: String
                    ) extends Module

object Module {
  type Coalgebra[F[_], A] = qq.droste.CoalgebraM[F, RoseTreeF[Module, ?], A]

  def moduleNameFromPath(path: Path): String = {
    FilenameUtils.removeExtension(path.getFileName.toString)
  }

  def fromFilesystem[M[_] : Monad](implicit fileSystem: FileSystem[M]): Coalgebra[M, Path] =
    CoalgebraM[M, CoattrF[List, Module, ?], Path] { path =>
      (fileSystem.isRegularFile(path), fileSystem.isDirectory(path)).tupled >>= {
        case (true, _) =>
          fileSystem.readAllText(path, StandardCharsets.UTF_8) map { sql => CoattrF.pure[List, Module, Path](SqlModule(sql)) }
        case (_, true) => fileSystem.list(path) map CoattrF.roll[List, Module, Path]
        case _ => CoattrF.roll[List, Module, Path](List.empty).pure[M]
      }
    }
}