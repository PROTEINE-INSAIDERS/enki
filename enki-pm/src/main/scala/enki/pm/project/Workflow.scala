package enki.pm.project

import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import enki.pm.cli._
import enki.pm.fs._
import io.chrisdavenport.log4cats._

case class Project()

case class ProjectServices[F[_]](
                                  prompt: Prompt[F]
                                )


object Workflow {
  private val enkiDir = ".enki"

  private def getProjectDir[M[_] : Monad](implicit fileSystem: FileSystem[M],
                                          logger: Logger[M],
                                          prompt: BootstrapPrompt[M]): M[Path] = {
    Paths.get(System.getProperty("user.dir")).tailRecM[M, Path] { path =>
      for {
        enkiDirExists <- fileSystem.isDirectory(path.resolve(enkiDir))
        res <- if (enkiDirExists)
          path.asRight.pure[M]
        else for {
          _ <- logger.info(s"No project found in `$path'")
          createNew <- prompt.createNewProject(path)
          res <- if (createNew) for {
            _ <- fileSystem.createDirectories(path.resolve(enkiDir))
            _ <- logger.info(s"Project created in $path")
          } yield path.asRight[Path]
          else
            prompt.projectDir fmap (_.asLeft[Path])
        } yield res
      } yield res
    }
  }

  private def createEnkiPrompt[M[_] : Monad, E](implicit console: Console[M],
                                                bracket: Bracket[M, E],
                                                fileSystem: FileSystem[M],
                                                logger: Logger[M]): M[Prompt[M]] = {
    implicit val recorder = NullRecorder[M]()
    (ConsolePrompt[M, E](): Prompt[M]).pure[M]
  }

  /**
    * Initialize enki project.
    */
  def bootstrap[M[_] : Monad : LiftIO](
                                        implicit bracket: Bracket[M, Throwable],
                                        logger: Logger[M],
                                        console: Console[M]
                                      ): M[Unit] = {


    implicit val prompt: BootstrapPrompt[M] = BootstrapPromptCli[M, Throwable]()
    implicit val fileSystem: FileSystem[M] = new NioFileSystem[M]()

    for {
      projectDir <- getProjectDir[M]
    } yield ()
  }

  /**
    * Build up project structure.
    */
  def buildUp[M[_] : Monad](project: Project): Unit = {
    ???
  }


}
