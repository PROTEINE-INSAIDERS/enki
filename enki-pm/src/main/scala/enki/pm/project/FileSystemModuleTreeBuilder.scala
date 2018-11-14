package enki.pm.project

import java.nio.charset._
import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import enki.pm.internal._
import org.apache.commons.io.FilenameUtils

//TODO: put ana here?
case class FileSystemModuleTreeBuilder[M[_], E <: Throwable](
                                                              implicit fileSystem: FileSystem[M],
                                                              monadError: MonadError[M, E]
                                                            ) extends ModuleTreeBuilder[M, Path] {

  override protected def step(path: Path, attributes: InheritedAttributes): M[RoseTreeF[Validated[Module], Path]] = {
    Validated.wrapError(
      (fileSystem.isRegularFile(path), fileSystem.isDirectory(path)).tupled >>= {
        case (true, _) => fileSystem.readAllText(path, StandardCharsets.UTF_8) map { sql =>
          RoseTreeF.leaf[Validated[Module], Path](sqlModule(sql, attributes))
        }
        case (_, true) => fileSystem.list(path) map { files =>
          if (files.nonEmpty)
            RoseTreeF.node[Validated[Module], Path](files)
          else
            RoseTreeF.leaf[Validated[Module], Path](s"Dirrectory $path is empty.".invalidNec)
        }
        case _ => RoseTreeF.leaf[Validated[Module], Path](s"$path neither file nor dirrectory.".invalidNec).pure[M]
      }
    ).map {
      case Validated.Valid(res) => res
      case Validated.Invalid(err) => RoseTreeF.leaf[Validated[Module], Path](err.invalid)
    }
  }

  override protected def getModuleName(path: Path): M[String] = FilenameUtils.removeExtension(path.getFileName.toString).pure[M]
}
