package enki.pm.project

import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import enki.pm.cli._
import enki.pm.fs._
import io.chrisdavenport.log4cats._

case class Project()


object Workflow {
  def bootstrap[M[_] : Monad : LiftIO](
                                        implicit bracket: Bracket[M, Throwable],
                                        logger: Logger[M],
                                        console: Console[M]
                                      ): M[Project] = {
    val enkiDir = ".enki"

    implicit val prompt: BootstrapPrompt[M] = BootstrapPromptCli[M, Throwable]()
    implicit val fileSystem: FileSystem[M] = new NioFileSystem[M]()

    logger.trace("Bootstrap phase") *> Paths.get(System.getProperty("user.dir")).tailRecM[M, Project] { path =>
      for {
        enkiDirExists <- fileSystem.isDirectory(path.resolve(enkiDir))
        res <- if (enkiDirExists)
          Either.right[Path, Project](Project()).pure[M]
        else for {
          _ <- logger.info(s"No project found in `$path'")
          createNew <- prompt.createNewProject(path)
          res <- if (createNew) for {
            _ <- fileSystem.createDirectories(path.resolve(enkiDir))
            _ <- logger.info(s"Project created in $enkiDir")
          } yield Either.right[Path, Project](Project())
          else
            prompt.projectDir fmap (Either.left[Path, Project](_))
        } yield res
      } yield res
    }
  }
}
