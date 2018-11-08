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
    implicit val questions: PromptQuestions = new PromptQuestions()
    implicit val parsers: PromptParsers = new PromptParsers()
    implicit val prompt: CliPrompt[M, Throwable] = new CliPrompt[M, Throwable]()
    implicit val fileSystem: NioFileSystem[M] = new NioFileSystem[M]()

    logger.trace("Bootstrap phase") *> Paths.get(".enki").tailRecM[M, Option[Project]] { path =>
        Either.right[Path, Option[Project]](None).pure[M]
      }
  }
}
