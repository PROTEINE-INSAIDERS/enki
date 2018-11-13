package enki.pm.cli

import java.nio.file._

import atto.Atto._
import atto._
import cats._
import cats.data._
import cats.effect._
import cats.implicits._

import scala.util._

trait BootstrapPrompt[F[_]] {
  def createNewProject(path: Path): F[Boolean]

  def projectDir: F[Path]
}

case class BootstrapPromptCli[F[_] : Monad, E](
                                                implicit console: Console[F],
                                                bracket: Bracket[F, E]
                                              ) extends BootstrapPrompt[F] with ConsolePromptFunctions {
  private val parsers: BootstrapPrompt[Parser] = BootstrapPromptParsers()

  private val questions: BootstrapPrompt[Const[String, ?]] = BootstrapPromptQuestions()

  implicit private val recorder: AnswerRecorder[F] = new AnswerRecorder[F] {
    override def get(key: String): F[Option[String]] = {
      val projectDir = questions.projectDir.getConst
      val doNotCreateInEnki2 = questions.createNewProject(Paths.get("/home/schernichkin/Projects/enki2")).getConst
      val createInEnkiTest = questions.createNewProject(Paths.get("/home/schernichkin/Projects/test-enki-project")).getConst
      (key match {
        case `projectDir` => Some("/home/schernichkin/Projects/test-enki-project")
        case `doNotCreateInEnki2` => Some("n")
        case `createInEnkiTest` => Some("y")
        case _ => Option.empty[String]
      }).pure[F]
    }

    override def store(key: String, value: String): F[Unit] = ().pure[F]
  }

  private def ask[A](question: Const[String, A], parser: Parser[A]) = ask[F, A, E](question.getConst, parser)

  override def projectDir: F[Path] = ask(questions.projectDir, parsers.projectDir)

  override def createNewProject(path: Path): F[Boolean] = ask(questions.createNewProject(path), parsers.createNewProject(path))
}

case class BootstrapPromptParsers() extends BootstrapPrompt[Parser] with CommonParsers {
  override def createNewProject(path: Path): Parser[Boolean] = bool

  override def projectDir: Parser[Path] = takeText >>= { str =>
    Try {
      Paths.get(str).pure[Parser]
    }.recover {
      case e: Throwable => err[Path](e.getLocalizedMessage)
    }.get
  }
}

case class BootstrapPromptQuestions() extends BootstrapPrompt[Const[String, ?]] {
  override def projectDir: Const[String, Path] = Const("Enter project directory:")

  override def createNewProject(path: Path): Const[String, Boolean] = Const(s"Would you like to create a new project in `$path'?")
}