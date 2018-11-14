package enki.pm.cli

import atto.Atto._
import atto._
import cats._
import cats.data._
import cats.effect._

case class ConsolePrompt[F[_] : Monad, E](
                                      implicit console: Console[F],
                                      bracket: Bracket[F, E],
                                      recorder: PromptRecorder[F]
                                    ) extends Prompt[F] with ConsolePromptFunctions {
  private val parsers: Prompt[Parser] = ConsolePromptParsers()

  private val questions: Prompt[Const[String, ?]] = ConsolePromptQuestions()

  private def ask[A](question: Const[String, A], parser: Parser[A]): F[A] = ask[F, A, E](question.getConst, parser)

  override def projectName: F[String] = ask(questions.projectName, parsers.projectName)

  override def sqlRoot: F[String] = ask(questions.sqlRoot, parsers.sqlRoot)
}

case class ConsolePromptParsers() extends Prompt[Parser] {
  override def sqlRoot: Parser[String] = takeText

  override def projectName: Parser[String] = takeText
}

case class ConsolePromptQuestions() extends Prompt[Const[String, ?]] {
  override def projectName: Const[String, String] = Const("Project name:")

  override def sqlRoot: Const[String, String] = Const("Where sql files located?")
}
