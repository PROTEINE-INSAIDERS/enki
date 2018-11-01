package enki.pm.project

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import enki.pm.internal._
import qq.droste._

class FileSystemModuleTreeBuilder[F[_] : FileSystem, E <: Throwable](
                                                                      implicit fileSystem: FileSystem[F],
                                                                      error: MonadError[F, E]
                                                                    ) {
  def coalgebra: CoalgebraM[F, RoseTreeF[Validated[Module], ?], Path] =
    CoalgebraM[F, RoseTreeF[Validated[Module], ?], Path] { path =>
      Validated.wrapError {
        (fileSystem.isRegularFile(path), fileSystem.isDirectory(path)).tupled >>= {
          case (true, _) => fileSystem.readAllText(path, StandardCharsets.UTF_8) map { sql =>
            RoseTreeF.leaf[Validated[Module], Path](SqlModule(sql).valid)
          }
          case (_, true) => fileSystem.list(path) map RoseTreeF.node[Validated[Module], Path]
          case _ => RoseTreeF.leaf[Validated[Module], Path](s"$path neither file nor dirrectory.".invalidNec).pure[F]
        }
      }.map {
        case Validated.Valid(res) => res
        case Validated.Invalid(err) => RoseTreeF.leaf[Validated[Module], Path](err.invalid)
      }
    }
}