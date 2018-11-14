package enki.pm.cli

import atto.Atto._
import atto._
import cats._
import cats.data._
import cats.effect._

case class ConsolePrompt[F[_] : Monad, E](
                                      implicit console: Console[F],
                                      bracket: Bracket[F, E],
                                      recorder: AnswerRecorder[F]
                                    ) extends Prompt[F] with ConsolePromptFunctions {
  private val parsers: Prompt[Parser] = ConsolePromptParsers()

  private val questions: Prompt[Const[String, ?]] = ConsolePromptQuestions()

  private def ask[A](question: Const[String, A], parser: Parser[A]): F[A] = ask[F, A, E](question.getConst, parser)

  override def scanDir: F[Boolean] = ask(questions.scanDir, parsers.scanDir)

  override def sqlRoot: F[String] = ask(questions.sqlRoot, parsers.sqlRoot)
}

case class ConsolePromptParsers() extends Prompt[Parser] with CommonParsers {
  override def sqlRoot: Parser[String] = takeText

  override def scanDir: Parser[Boolean] = bool
}

case class ConsolePromptQuestions() extends Prompt[Const[String, ?]] {
  override def scanDir: Const[String, Boolean] = Const("Would you like to scan project directory?")

  override def sqlRoot: Const[String, String] = Const("Where sql files located?")
}
