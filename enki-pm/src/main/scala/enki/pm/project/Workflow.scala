package enki.pm.project

import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import enki.pm.cli._
import enki.pm.fs.NioFileSystem
import io.chrisdavenport.log4cats._

case class Project()

object Workflow {
  def bootstrap[M[_] : Monad : LiftIO](
                                        implicit bracket: Bracket[M, Throwable],
                                        logger: Logger[M],
                                        console: Console[M]
                                      ): M[Option[Project]] = {
    val enkiDir = ".enki"

    implicit val questions: PromptQuestions = new PromptQuestions()
    implicit val parsers: PromptParsers = new PromptParsers()
    implicit val prompt: Prompt[M] = new CliPrompt[M, Throwable]()
    implicit val fileSystem: NioFileSystem[M] = new NioFileSystem[M]()

    logger.trace("Bootstrap phase") *> Paths.get(System.getProperty("user.dir")).tailRecM[M, Option[Project]] { path =>
      fileSystem.isDirectory(path.resolve(enkiDir)) >>= { isDirrectory =>
        if (isDirrectory) {
          logger.error("load project") *> Either.right[Path, Option[Project]](None).pure[M]
        }
        else {
          logger.info(s"No project found in `$path'") *> prompt.createNewProject(path) >>= { createNew => {
            if (createNew) {

              logger.error("create new") *> Either.right[Path, Option[Project]](None).pure[M]
            } else {
              prompt.projectDir fmap { Either.left[Path, Option[Project]](_) }
            }
          }
          }
        }
      }
    }
  }
}
