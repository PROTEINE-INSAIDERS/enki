package enki.pm.project

import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._
import enki.pm.cli._
import enki.pm.fs._
import io.chrisdavenport.log4cats._
import io.circe.parser._
import io.circe.syntax._

case class Project()

case class ProjectServices[F[_]](
                                  prompt: Prompt[F]
                                )

object Workflow {

  private def getProjectFileSystem[M[_] : Monad](implicit fileSystem: FileSystem[M],
                                                 logger: Logger[M],
                                                 prompt: BootstrapPrompt[M]): M[ProjectFileSystem[M]] = {

    Paths.get(System.getProperty("user.dir")).tailRecM[M, ProjectFileSystem[M]] { path =>
      for {
        enkiDirExists <- fileSystem.isDirectory(path.resolve(ProjectFileSystem.enkiDir))
        res <- if (enkiDirExists)
          ProjectFileSystem[M](fileSystem = fileSystem, projectDir = path).asRight.pure[M]
        else for {
          _ <- logger.info(s"No project found in `$path'")
          createNew <- prompt.createNewProject(path)
          res <- if (createNew) for {
            _ <- fileSystem.createDirectories(path.resolve(ProjectFileSystem.enkiDir))
            _ <- logger.info(s"Project created in $path")
          } yield ProjectFileSystem[M](fileSystem = fileSystem, projectDir = path).asRight[Path]
          else
            prompt.projectDir fmap (_.asLeft[ProjectFileSystem[M]])
        } yield res
      } yield res
    }
  }

  private def createPrompt[F[_] : Sync](
                                         answerPath: Path
                                       )
                                       (
                                         implicit fileSystem: FileSystem[F],
                                         logger: Logger[F],
                                         bootstrapPrompt: BootstrapPrompt[F],
                                         console: Console[F]
                                       ): Resource[F, Prompt[F]] = {
    def acquire: F[CacheRecorder[F]] = for {
      fileExists <- fileSystem.isRegularFile(answerPath)
      answers <- if (fileExists) {
        fileSystem.readAllText(answerPath) >>= (decode[Map[String, String]](_).leftMap(a => a: Throwable).raiseOrPure[F])
      } else {
        Map.empty[String, String].pure[F]
      }
      answersRef <- Ref.of(answers)
    } yield CacheRecorder(answersRef)

    def saveAnswers(recorder: CacheRecorder[F]): F[Unit] = for {
      answers <- recorder.answers.get
      _ <- fileSystem.writeAllText(answerPath, answers.asJson.spaces2)
      _ <- logger.info(s"Answers saved to `$answerPath'.")
    } yield ()

    def askAndSave(recorder: CacheRecorder[F]): F[Unit] =
      bootstrapPrompt.saveAnswerFile >>= { save => if (save) saveAnswers(recorder) else ().pure[F] }

    def release(recorder: CacheRecorder[F], e: ExitCase[Throwable]): F[Unit] = e match {
      case ExitCase.Completed =>
        saveAnswers(recorder)
      case ExitCase.Canceled =>
        logger.warn("Releasing answer recorder due to cancelation.") >> askAndSave(recorder)
      case ExitCase.Error(e) =>
        logger.warn(s"Releasing answer recorder due to error: $e") >> askAndSave(recorder)
    }

    Resource.makeCase(acquire)(release) fmap { implicit recorder => ConsolePrompt[F, Throwable]() }
  }

  def scan[F[_] : Monad](
                          implicit projectFileSystem: ProjectFileSystem[F],
                          error: MonadError[F, Throwable],
                          prompt: Prompt[F]
                        ): F[Unit] = for {
    s <- prompt.scanDir
    _ <- if (s) {
      val moduleTreeBuilder = imply(projectFileSystem.fileSystem)(FileSystemModuleTreeBuilder[F, Throwable])

      ???
    } else {
      ().pure[F]
    }
  } yield ()

  def bootstrap[F[_] : Sync : LiftIO](
                                       implicit logger: Logger[F],
                                       console: Console[F]
                                     ): F[Unit] = {
    implicit val bootstrapPrompt: BootstrapPrompt[F] = BootstrapPromptCli[F, Throwable]()
    implicit val fileSystem: FileSystem[F] = new NioFileSystem[F]()

    for {
      projectFilesystem <- getProjectFileSystem[F]
      prompt = createPrompt[F](projectFilesystem.answerFileLocation)
      _ <- prompt.use { prompt => imply(projectFilesystem, prompt)(scan[F]) }
    } yield ()
  }
}
